from passari.exceptions import PreservationError

from subprocess import CalledProcessError


class ErrorDetector:
    """
    Base class for a detector that checks the exception and raises
    a PreservationError if applicable
    """
    def check(self, exc):
        raise NotImplementedError


class JPEGVersionError(ErrorDetector):
    """
    Raise a PreservationError if the JPEG version couldn't be detected
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        jpeg_version_error = (
            ("ValueError: MIME type image/jpeg with version JPEG image "
             "data is not supported.")
            in exc.stderr.decode("utf-8")
        )

        if jpeg_version_error:
            # JPEG images with metadata are detected incorrectly although they
            # are currently supported by DPRES service. EXIF 2.3 is unsupported
            # at the moment, but will be added during the next spec revision.
            # Migration isn't necessary, so freeze failing JPEG files for the
            # time being.
            # (CSC ticket #366381)
            # TODO: Remove this once JPEG version detection is fixed in
            # `file-scraper`.
            raise PreservationError(
                detail=(
                    f"JPEG file {exc.cmd[-1]} couldn't be detected correctly, "
                    f"and it may use EXIF metadata which isn't detected "
                    f"properly yet."
                ),
                error="JPEG version not detected correctly"
            )


class JHOVEInvalidTIFFError(ErrorDetector):
    """
    Raise a PreservationError if JHOVE detects an invalid TIFF.
    This most likely means the issue is with the file and not the validator
    considering that JHOVE's TIFF validator is used widely.
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        stderr = exc.stderr.decode("utf-8")

        if "Validator returned error" not in stderr:
            return

        # TODO: Maybe parse the actual XML output?
        # However, dpres-siptools output may not be stable, so it could be
        # a flaky solution without much benefit for now.

        # Error was produced by JHOVE's TIFF-hul report module
        is_tiff_error = ">TIFF-hul</reportingModule>" in stderr

        if is_tiff_error:
            raise PreservationError(
                detail=(
                    f"TIFF file {exc.cmd[-1]} failed JHOVE validation, and "
                    f"is likely invalid."
                ),
                error="TIFF file failed JHOVE validation"
            )


class MultiPageTIFFError(ErrorDetector):
    """
    Raise a PreservationError if multi-page TIFF fails validation.
    Multi-page TIFFs are not currently supported by the DPRES service.
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        file_path = exc.cmd[-1]
        file_ext = file_path.split(".")[-1].lower()

        if file_ext not in ("tif", "tiff"):
            return

        stderr = exc.stderr.decode("utf-8")

        is_multipage = (
            "The file contains multiple streams which is supported only for "
            "video containers." in stderr
        )

        if is_multipage:
            raise PreservationError(
                detail=(
                    f"TIFF file {exc.cmd[-1]} contains multiple pages and is "
                    f"not currently allowed for preservation."
                ),
                error="Multi-page TIFF not allowed"
            )


class JPEGMIMETypeError(ErrorDetector):
    """
    Raise a PreservationError if MIME type for JPEG isn't detected correctly
    due to an issue in file-scraper's PilScraper module.
    See CSC ticket #400408
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        file_path = exc.cmd[-1]
        file_ext = file_path.split(".")[-1].lower()

        if file_ext not in ("jpg", "jpeg"):
            return

        stderr = exc.stderr.decode("utf-8")

        mime_type_detection_failed = \
            "MIME type not supported by this scraper." in stderr

        if mime_type_detection_failed:
            raise PreservationError(
                detail=(
                    f"JPEG file {exc.cmd[-1]} didn't pass MIME type detection"
                ),
                error="JPEG MIME type detection failed"
            )


ERROR_DETECTORS = (
    JPEGVersionError, JHOVEInvalidTIFFError, MultiPageTIFFError,
    JPEGMIMETypeError
)

# TODO: As with event creators, error detectors could be developed and
# deployed independently using setuptools' entrypoint feature, if this becomes
# necessary.


def raise_for_preservation_error(exc: CalledProcessError):
    """
    Check if the failed subprocess call was caused by a common preservation
    error and raise a PreservationError if so.

    This allows the workflow to automatically handle known errors instead
    of cluttering the list of failed jobs.
    """
    for error_cls in ERROR_DETECTORS:
        error_detector = error_cls()
        error_detector.check(exc)