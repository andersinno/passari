from passari.exceptions import PreservationError

from subprocess import CalledProcessError


class ErrorDetector:
    """
    Base class for a detector that checks the exception and raises
    a PreservationError if applicable
    """
    def check(self, exc):
        raise NotImplementedError


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


class JPEGMPONotSupportedError(ErrorDetector):
    """
    Raise a PreservationError if a MPO file (image format based on JPEG using
    the same file extension) is detected.
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        file_path = exc.cmd[-1]
        file_ext = file_path.split(".")[-1].lower()

        if file_ext not in ("jpg", "jpeg"):
            return

        stderr = exc.stderr.decode("utf-8")

        mpo_found = (
            "Conflict with values 'image/jpeg' and 'image/mpo' for 'mimetype'"
        ) in stderr

        if mpo_found:
            raise PreservationError(
                detail=(
                    f"MPO image file {exc.cmd[-1]} is not supported"
                ),
                error="MPO JPEG files not supported"
            )


class JPEGVersionNotSupportedError(ErrorDetector):
    """
    Raise a PreservationError if the file format version couldn't be detected
    for a JPEG file.

    In the cases discovered so far, file-scraper is not able to detect any
    JPEG version at all (eg. the version is "(:unav)"), resulting in the
    "version not supported" error.

    The faulty files were also created using Photoshop CS5 and CS6, and were
    converted from TIFF to JPEG.
    """
    def check(self, exc):
        if exc.cmd[0] != "import-object":
            return

        file_path = exc.cmd[-1]
        file_ext = file_path.split(".")[-1].lower()

        if file_ext not in ("jpg", "jpeg"):
            return

        stderr = exc.stderr.decode("utf-8")

        version_not_detected = (
            "File format version is not supported." in stderr
        )

        if version_not_detected:
            raise PreservationError(
                detail=(
                    f"JPEG version not supported for {file_path}"
                ),
                error="JPEG version not supported"
            )


ERROR_DETECTORS = (
    JHOVEInvalidTIFFError, JPEGMIMETypeError,
    JPEGMPONotSupportedError, JPEGVersionNotSupportedError
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
