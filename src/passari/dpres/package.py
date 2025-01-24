import asyncio
import datetime
import os
import shutil
import subprocess
from pathlib import Path

import lxml.etree

import aiofiles

from mets_builder import (
    METS,
    MetsProfile,
    MetsRecordStatus,
)
from mets_builder.metadata import (
    DigitalProvenanceAgentMetadata,
    DigitalProvenanceEventMetadata,
    ImportedMetadata,
    MetadataFormat,
    MetadataType,
)
from siptools_ng.file import File
from siptools_ng.sip import SIP

from passari.config import (CONTRACT_ID, LIDO_REPORT_ID, MUSEUMPLUS_URL,
                            ORGANIZATION_NAME, SIGN_KEY_PATH)
from passari.dpres.events import PremisEvent, get_premis_events
from passari.dpres.scripts import extract_archive
from passari.exceptions import PreservationError
from passari.logger import logger
from passari.museumplus.connection import get_museum_session
from passari.museumplus.db import (MuseumAttachment, MuseumCollectionActivity,
                                   MuseumObject)
from passari.utils import gather_or_raise_first, retrieve_cached_xml

IMAGE_FORMATS = set(["gif", "tif", "tiff", "jpg", "jpeg"])
DOCUMENT_FORMATS = set([
    "pdf",  # TODO: PDF is defined as both transmission (if not PDF/A)
            # and storage (if PDF/A) capable. Should we check for PDF/A
            # compliance also?
    "odf", "xml"
])
MISC_FORMATS = set([
    # 'attachment' is used as a placeholder file format, and it may
    # be of any type
    "attachment"
])
SUPPORTED_FORMATS = IMAGE_FORMATS | DOCUMENT_FORMATS | MISC_FORMATS

ARCHIVE_FORMATS = set(["zip"])

CHUNK_SIZE = 8192
ARCHIVE_PART_LENGTH = 3


def get_archive_path_parts(object_id: int, sip_filename: str) -> list:
    """
    Get archive path parts for an object ID with the given SIP filename

    This function exists to get the path to the archived log files
    after a package has finished processing and has otherwise been removed
    entirely from the system.
    """
    object_id = str(object_id)

    id_parts = [
        object_id[i:i+ARCHIVE_PART_LENGTH] for i
        in range(0, len(object_id), ARCHIVE_PART_LENGTH)
    ]

    return id_parts + [f"Object_{object_id}", sip_filename]


class MuseumObjectPackage:
    """
    Museum object representing an object and attachments downloaded
    from the MuseumPlus database into a local directory
    """
    def __init__(self, path, museum_object=None, sip_id=None):
        """
        :param path: Path that contains the object and related files
        :param MuseumObject museum_object: Existing MuseumObject instance,
                                           if available
        :param sip_id: Optional SIP ID added into the SIP filename to
                       differentiate multiple packaged SIPs
        """
        self.path = Path(path)

        self.workspace_dir.mkdir(exist_ok=True)
        self.sip_dir.mkdir(exist_ok=True)
        self.report_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

        self.all_files = []

        self.attachments = None
        self.collection_activities = None

        self.museum_object = museum_object
        self.sip_id = sip_id

        self.sip_agent_md = DigitalProvenanceAgentMetadata(
            name="passari",
            agent_type="software",
            version="0.0.0"
        )

    @classmethod
    async def from_path(self, path, sip_id=None):
        """
        Load a MuseumObjectPackage from the path and setup a HTTP session
        to retrieve files from MuseumPlus
        """
        museum_package = MuseumObjectPackage(path=path, sip_id=sip_id)
        museum_package.load_museum_object()
        museum_package.load_attachments()
        museum_package.load_collection_activities()
        await museum_package.load_museum_session()

        return museum_package

    @classmethod
    def from_path_sync(self, path, sip_id=None):
        """
        Load a MuseumObjectPackage from the path without setting up a HTTP
        session.

        This is useful if we only need to access path attributes such as
        'workspace_dir'
        """
        museum_package = MuseumObjectPackage(path=path, sip_id=sip_id)
        museum_package.load_museum_object()
        museum_package.load_attachments()
        museum_package.load_collection_activities()

        return museum_package

    @property
    def session(self):
        return self.museum_object.session

    @property
    def archive_path_parts(self):
        """
        Path parts that comprise a directory structure under which
        logs and files can be saved.

        This is used because storing millions of files/directories under
        a single directory would lead to performance issues.
        """
        return get_archive_path_parts(
            object_id=self.museum_object.object_id,
            sip_filename=self.sip_filename
        )

    def set_attachments(self, attachments):
        self._attachments = attachments

    def get_attachments(self):
        if self._attachments is None:
            raise ValueError("Attachments haven't been downloaded yet.")

        return self._attachments

    def set_collection_activities(self, collection_activities):
        self._collection_activities = collection_activities

    def get_collection_activities(self):
        if self._collection_activities is None:
            raise ValueError(
                "Collection activities haven't been downloaded yet."
            )

        return self._collection_activities

    async def load_museum_session(self):
        self.museum_object.session = await get_museum_session()

    async def close(self):
        """
        Close the MuseumPlus session
        """
        if self.museum_object:
            await self.museum_object.close()

    def load_museum_object(self):
        """
        Load the underlying MuseumObject instance
        """
        # Load the XML file that has already been saved into the directory
        with open(self.report_dir / "Object.xml", "rb") as f:
            etree = lxml.etree.fromstring(f.read())

        self.museum_object = MuseumObject(
            etree=etree, session=None
        )

    async def download_attachment(self, item_id: int) -> MuseumAttachment:
        """
        Download an attachment if it hasn't been downloaded already and
        return a MuseumAttachment instance
        """
        attachment_dir = self.attachment_dir / str(item_id)
        attachment_dir.mkdir(exist_ok=True, parents=True)

        etree = await retrieve_cached_xml(
            session=self.session,
            url=f"{MUSEUMPLUS_URL}/module/Multimedia/{item_id}",
            path=attachment_dir / f"Multimedia.xml"
        )
        attachment = MuseumAttachment(etree)

        filename = attachment.filename

        if filename == "Multimedia.xml":
            # If the attachment is named "Multimedia.xml", it would overwrite
            # the XML document we have already saved in the same directory.
            # In this case, raise a PreservationError to prevent packaging
            # and the file from being silently overwritten.
            raise PreservationError(
                detail=(
                    "The attachment filename 'Multimedia.xml' conflicts with "
                    "the Multimedia XML document we have already downloaded."
                ),
                error="Filename 'Multimedia.xml' not allowed for attachment"
            )

        # Raise PreservationError if attachment filename contains non-ASCII
        # characters, as those are not yet supported by the DPRES service.
        # See CSC ticket #402027.
        # TODO: Remove once the filename issue at DPRES service is fixed
        try:
            filename.encode("ascii")
        except UnicodeEncodeError:
            raise PreservationError(
                detail=(
                    f"Filename {filename} contains non-ASCII characters and "
                    f"can't be uploaded into the DPRES service at this time."
                ),
                error="Filename contains non-ASCII characters"
            )

        attachment_path = attachment_dir / attachment.filename

        # Only download the attachment if it doesn't exist already
        # This way repeated download attempts don't redownload files
        # we already have
        if not os.path.exists(attachment_path):
            # TODO: Can we use anything else to determine whether the file
            # is complete? MuseumPlus doesn't seem to provide a file size
            # or a checksum.
            logger.info(
                f"Downloading attachment {attachment.filename} for object "
                f"{self.museum_object.object_id}"
            )
            # Use a temporary filename during download so we don't mistake
            # half-finished downloads for complete files
            temp_path = attachment_path.with_suffix(
                f"{attachment_path.suffix}.download"
            )

            response = await self.session.get(
                f"{MUSEUMPLUS_URL}/module/Multimedia/{item_id}/attachment",
                headers={"Accept": "application/octet-stream"}
            )
            if response.status == 404:
                # No attachment exists for this Multimedia instance; only
                # return the metadata
                return attachment

            response.raise_for_status()

            # TODO: If we can determine the file size beforehand, we could use
            # fallocate to allocate the required disk space and then
            # download the file.
            async with aiofiles.open(temp_path, "wb") as file_:
                while True:
                    chunk = await response.content.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    await file_.write(chunk)

            if temp_path.stat().st_size == 0:
                # File is empty; remove it and only package the metadata XML
                logger.debug(
                    f"Deleting empty attachment {attachment.filename}"
                )
                temp_path.unlink()
                return attachment

            # Download finished; we can safely rename it now
            os.rename(temp_path, attachment_path)
        else:
            logger.debug(
                f"Skipping existing attachment {attachment.filename}"
            )

        return attachment

    async def download_attachments(self):
        """
        Download all the attachments for an object in parallel
        """
        tasks = [
            self.download_attachment(item_id)
            for item_id in self.museum_object.attachment_ids
        ]

        attachments = await gather_or_raise_first(*tasks)
        self.attachments = attachments

        # Remove files that were downloaded before but no longer belong
        # to this object
        self.clean_attachments()

    def load_attachment(self, item_id: int):
        """
        Load the Multimedia XML document for an attachment and return
        a MuseumAttachment instance if it exists
        """
        attachment_xml_path = (
            self.attachment_dir / str(item_id) / f"Multimedia.xml"
        )

        try:
            with open(attachment_xml_path, "rb") as file_:
                data = file_.read()
                etree = lxml.etree.fromstring(data)
                return MuseumAttachment(etree)
        except FileNotFoundError:
            return None

    def load_attachments(self):
        """
        Check for attachments that have already been downloaded and populate
        the `attachments` list
        """
        attachments = [
            self.load_attachment(item_id)
            for item_id in self.museum_object.attachment_ids
        ]

        self.attachments = [
            attachment for attachment in attachments if attachment
        ]

    def load_collection_activity(self, item_id: int):
        """
        Load the CollectionActivity XML document and return a
        MuseumCollectionActivity instance if it exists
        """
        col_activity_xml_path = (
            self.collection_activity_dir / str(item_id)
            / f"CollectionActivity.xml"
        )

        try:
            with open(col_activity_xml_path, "rb") as file_:
                data = file_.read()
                etree = lxml.etree.fromstring(data)
                return MuseumCollectionActivity(etree)
        except FileNotFoundError:
            return None

    def load_collection_activities(self):
        """
        Check for collection activities that have already been downloaded
        and populate the `collection_activities` list
        """
        collection_activities = [
            self.load_collection_activity(item_id)
            for item_id in self.museum_object.collection_activity_ids
        ]

        self.collection_activities = [
            col_activity for col_activity in collection_activities
            if col_activity
        ]

    def clean_attachments(self):
        """
        Remove attachment directories that are no longer part of the
        museum object
        """
        attachment_ids = set([
            str(attachment.attachment_id) for attachment in
            self.attachments
        ])

        for path in self.attachment_dir.glob("*"):
            if path.is_dir() and path.name not in attachment_ids:
                shutil.rmtree(path)

    async def download_collection_activities(self):
        """
        Download all the collection activity entries in parallel
        """
        tasks = [
            self.download_collection_activity(item_id)
            for item_id in self.museum_object.collection_activity_ids
        ]

        collection_activities = await gather_or_raise_first(*tasks)
        self.collection_activities = collection_activities

        self.clean_collection_activities()

    async def download_collection_activity(self, item_id: int):
        """
        Download a collection activity document if it hasn't been downloaded
        already and return a MuseumCollectioActivity instance
        """
        collection_activity_dir = self.collection_activity_dir / str(item_id)
        collection_activity_dir.mkdir(exist_ok=True, parents=True)

        etree = await retrieve_cached_xml(
            session=self.session,
            url=f"{MUSEUMPLUS_URL}/module/CollectionActivity/{item_id}",
            path=collection_activity_dir / f"CollectionActivity.xml"
        )
        return MuseumCollectionActivity(etree)

    def clean_collection_activities(self):
        """
        Remove collection activity directories that are no longer part of
        the museum object
        """
        collection_activity_ids = set([
            str(act.collection_activity_id) for act in
            self.collection_activities
        ])

        for path in self.collection_activity_dir.glob("*"):
            if path.is_dir() and path.name not in collection_activity_ids:
                shutil.rmtree(path)

    async def download_reports(self):
        """
        Save Object and LIDO documents under the report directory
        """
        await retrieve_cached_xml(
            session=self.session,
            url=(
                f"{MUSEUMPLUS_URL}/module/Object/{self.museum_object.object_id}/"
                f"export/{LIDO_REPORT_ID}"
            ),
            path=self.report_dir / "lido.xml"
        )

        # Save the object XML into the file system directly, since
        # MuseumObject contains it
        with open(self.report_dir / "Object.xml", "wb") as file_:
            file_.write(self.museum_object.tostring())

    def check_files(self):
        """
        Check that all files in the SIP are supported
        """
        for file_ in self.all_files:
            suffix = file_.suffix[1:].lower()

            if suffix not in SUPPORTED_FORMATS:
                raise PreservationError(
                    detail=(
                        f"File format {suffix} in SIP {self.sip_filename} "
                        "not supported for preservation."
                    ),
                    error=f"Unsupported file format: {suffix}"
                )

    def populate_files(self):
        """
        Populate the list of files contained in the package.

        This is done to continue from an interrupted process during which
        some files were downloaded already
        """
        files = (
            list(self.report_dir.glob("**/*")) +
            list(self.attachment_dir.glob("**/*"))
        )
        self.all_files = [f for f in files if f.is_file()]

    def copy_log_files_to_archive(self, archive_dir):
        """
        Copy log files to the archive, where they can be retrieved
        """
        archive_dir = Path(archive_dir)
        package_archive_dir = archive_dir.joinpath(
            *self.archive_path_parts
        )

        # Create the archival directory if one doesn't exist already
        package_archive_dir.mkdir(parents=True, exist_ok=True)

        # Compress the existing log files using gzip, replacing existing
        # files with .gz suffixed copies. This is idempotent.
        subprocess.run([
            "gzip", "--recursive", str(self.log_dir)
        ], check=True)

        # Copy the log files over
        subprocess.run([
            "cp", "-r",
            str(self.log_dir),
            str(package_archive_dir)
        ], check=True)

    @property
    def image_files(self):
        return [
            name for name in self.all_files
            if name.suffix[1:].lower() in IMAGE_FORMATS
        ]

    @property
    def archive_files(self):
        return [
            name for name in self.all_files
            if name.suffix[1:].lower() in ARCHIVE_FORMATS
        ]

    def iterate_files_to_import(self):
        """
        Iterate files to import by generating (identifier, file_path)
        tuples where `identifier` is an identifier for the file
        and `file_path` a path to the file itself
        """
        # Iterate the attachments first
        for attachment in self.attachment_dir.glob("*"):
            attachment_id = attachment.name
            attachment_dir = self.attachment_dir / attachment_id

            for file_path in attachment_dir.glob("**/*"):
                relative_path = file_path.relative_to(attachment_dir)
                yield (
                    f"Multimedia:{attachment_id}:{relative_path}",
                    file_path
                )

        # Iterate the collection activities
        for col_activity in self.collection_activity_dir.glob("*"):
            col_activity_id = col_activity.name
            col_activity_dir = self.collection_activity_dir / col_activity_id

            for file_path in col_activity_dir.glob("*"):
                relative_path = file_path.relative_to(col_activity_dir)
                yield (
                    f"CollectionActivity:{col_activity_id}:{relative_path}",
                    file_path
                )

        # Iterate the reports
        for report_path in self.report_dir.glob("*"):
            relative_path = report_path.relative_to(self.sip_dir)
            yield (
                f"Object:{self.museum_object.object_id}:{relative_path}",
                report_path
            )

    @property
    def sip_dir(self):
        """
        Path to the directory containing the SIP contents
        """
        return self.path / "sip"

    @property
    def report_dir(self):
        """
        Path to the directory containing the reports inside the SIP
        """
        return self.sip_dir / "reports"

    @property
    def log_dir(self):
        """
        Path to the directory containing log files generated during SIP
        creation
        """
        return self.path / "logs"

    @property
    def attachment_dir(self):
        """
        Path to the directory containing attachments inside the SIP
        """
        return self.sip_dir / "attachments"

    @property
    def collection_activity_dir(self):
        """
        Path to the directory containing collection activity entries inside the
        SIP
        """
        return self.sip_dir / "collection_activities"

    @property
    def workspace_dir(self):
        """
        Workspace directory path.

        Workspace directory is used by dpres-siptools in order to generate a
        SIP
        """
        return self.path / "workspace"

    @property
    def objid(self):
        """
        OBJID METS attribute used as an unique identifier for each SIP.

        The same OBJID is retained between different versions of the same
        object.
        """
        return f"Passari_Object_{self.museum_object.object_id}"

    @property
    def contentid(self):
        """
        CONTENTID METS attribute used as an unique identifier for each
        object.
        """
        return f"Object_{self.museum_object.object_id}"

    @property
    def sip_filename(self):
        """
        Filename used for the SIP archive.

        If 'sip_id' was provided, it is added to the filename
        to allow multiple SIPs to be created for the same package version.
        """
        # If 'sip_id' was provided, append it to the filename to ensure
        # multiple SIPs can be created for the same museum object
        sip_id = f"-{self.sip_id}" if self.sip_id else ""
        return f"{self.museum_object.sip_name}{sip_id}.tar"

    @property
    def sip_archive_path(self):
        """
        Path to the finished SIP archive
        """
        return self.path / self.sip_filename

    def clear_workspace(self):
        """
        Clear the workspace to ensure SIP creation can be started from scratch.
        """
        try:
            shutil.rmtree(self.workspace_dir)
        except FileNotFoundError:
            pass

        self.workspace_dir.mkdir(exist_ok=True)

    async def extract_archive(self, file_path):
        """
        Extract an archive and replace the original archive file
        with a directory containing the archive contents
        """
        # Rename the archive to allow a directory with the same name to be
        # created
        destination_path = file_path.with_suffix(f"{file_path.suffix}.extract")
        destination_path.mkdir()

        await extract_archive(
            path=file_path,
            destination_path=destination_path,
            log_path=self.log_dir / "extract-archive.log"
        )

        # Replace the original archive with an identically named directory
        # containing the extracted files
        os.remove(file_path)
        os.rename(destination_path, file_path)

    def create_mets(
        self,
        create_date: datetime,
        modify_date: datetime,
        update: bool
    ) -> METS:
        """
        Create the base METS object for the SIP using dpres-mets-builder

        https://digital-preservation-finland.github.io/dpres-mets-builder/usage.html
        """
        return METS(
            mets_profile=MetsProfile.CULTURAL_HERITAGE,
            contract_id=CONTRACT_ID,
            creator_name=ORGANIZATION_NAME,
            creator_type="ORGANIZATION",
            content_id=self.contentid,
            package_id=self.objid,
            create_date=create_date,
            last_mod_date=modify_date,
            record_status=(
                MetsRecordStatus.UPDATE if update else MetsRecordStatus.SUBMISSION
            )
        )

    def add_premis_object_identifier(self, file: File, identifier: str):
        """
        Add objectIdentifierType and objectIdentifierValue to the
        technical metadata of a PREMIS file object.
        """
        try:
            tech_md = next(
                metadata for metadata in file.metadata
                if metadata.metadata_type.value == "technical"
                and metadata.metadata_format.value == "PREMIS:OBJECT"
            )
        except StopIteration:
            raise ValueError(
                f"No technical metadata with format 'PREMIS:OBJECT' found in file "
                f"'{file.digital_object_path}'."
            )
        tech_md.object_identifier_type = "museumplus"
        tech_md.object_identifier = identifier

    def create_sip_from_data_dir(self, mets: METS) -> SIP:
        """
        Create the SIP from the data directory.

        dpres-siptools-ng generates the structmap and
        technical metadata for each file automatically.
        """
        dir_path = Path(self.sip_dir)

        if not dir_path.exists():
            raise ValueError(f"Path '{str(dir_path)}' does not exist.")
        if not dir_path.is_dir():
            raise ValueError(
                f"Path '{str(dir_path)}' is not a directory."
            )

        files = []
        for identifier, file_path in self.iterate_files_to_import():
            if file_path.is_file():
                file = File(
                    path=file_path,
                    digital_object_path=file_path.relative_to(dir_path.parent),
                )
                file.generate_technical_metadata()
                # Add object identifier to the technical metadata
                self.add_premis_object_identifier(file, identifier)
                files.append(file)

        return SIP.from_files(mets=mets, files=files)

    def add_dmd_to_sip(self, sip: SIP):
        """
        Create the descriptive metadata from lido.xml and
        add it to the SIP (the METS document).

        dres-siptools-ng will create a PREMIS event for this.
        """
        dmd = ImportedMetadata(
            metadata_type=MetadataType.DESCRIPTIVE,
            metadata_format=MetadataFormat.LIDO,
            format_version="1.1",
            data_path=self.report_dir / "lido.xml"
        )
        sip.add_metadata([dmd])

    def add_premis_events_to_sip(self, sip, files_extracted):
        """
        Create the provenance metadata for the PREMIS events
        and add them to the SIP (METS document). If any archives
        were extracted, add an event for that too.

        The Passari PREMIS agent metadata is linked to
        each event and doesn't need to be added explicitly.
        """
        events = get_premis_events(museum_package=self)

        if files_extracted:
            events.append(
                PremisEvent(
                    event_type="decompression",
                    event_detail="Archive extraction",
                    event_outcome="success",
                    event_outcome_detail=(
                        "Extracted from original archive downloaded "
                        "from MuseumPlus"
                    ),
                    event_datetime=datetime.datetime.now(
                        datetime.timezone.utc
                    )
                )
            )

        for event in events:
            event_md = DigitalProvenanceEventMetadata(
                event_type=event.event_type,
                detail=event.event_detail,
                outcome=event.event_outcome,
                outcome_detail=event.event_outcome_detail,
                datetime=event.event_datetime.isoformat()
            )
            event_md.link_agent_metadata(
                agent_metadata=self.sip_agent_md,
                agent_role="executing program"
            )
            sip.add_metadata([event_md])

    async def generate_sip(
            self,
            create_date: datetime.datetime = None,
            modify_date: datetime.datetime = None,
            update: bool = False):
        """
        Generate a SIP by following the guidelines described in the dpres-siptools-ng
        documentation:

        https://digital-preservation-finland.github.io/dpres-siptools-ng/examples.html

        :param update: Whether to create an update SIP.
        """
        if not create_date:
            create_date = datetime.datetime.now(datetime.timezone.utc)

        # Workspace needs to be cleaned to ensure no duplicate entries
        # remain after a previous run
        self.clear_workspace()
        self.populate_files()

        # Extract any archives
        files_extracted = False
        if self.archive_files:
            for file_path in self.archive_files:
                await self.extract_archive(file_path)

            # Populate the files again after extracting
            self.populate_files()
            files_extracted = True

        # Check that all files can be preserved
        self.check_files()

        # Create the base METS object
        mets = self.create_mets(create_date, modify_date, update)

        # Create the SIP from the source directory
        sip = self.create_sip_from_data_dir(mets)

        # Add descriptive metadata, i.e. the LIDO metadata into the SIP
        self.add_dmd_to_sip(sip)

        # Create provenance metadata for the PREMIS events and add them to the SIP
        self.add_premis_events_to_sip(sip, files_extracted)

        # Sign the METS and compress the SIP into a TAR file,
        # making it ready for submission to PAS
        sip.finalize(
            output_filepath=self.sip_archive_path,
            sign_key_filepath=SIGN_KEY_PATH
        )

    attachments = property(get_attachments, set_attachments)
    collection_activities = property(
        get_collection_activities, set_collection_activities
    )
