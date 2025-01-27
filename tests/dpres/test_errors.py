import pytest
from passari.exceptions import PreservationError


@pytest.mark.asyncio
async def test_generate_sip_unsupported_file_format(
        package_dir, museum_package_factory):
    """
    Test generating a SIP with an unsupported file format and ensure
    PreservationError is raised
    """
    museum_package = await museum_package_factory("1234570")

    with pytest.raises(PreservationError) as exc:
        await museum_package.generate_sip()

    assert "Unsupported file format: wad" == exc.value.error


@pytest.mark.asyncio
async def test_generate_sip_invalid_tiff_jhove(
        package_dir, museum_package_factory):
    """
    Test generating a SIP containing a TIFF file known to be invalid
    by JHOVE, and ensure an error is raised
    """
    museum_package = await museum_package_factory("1234577")

    with pytest.raises(ValueError) as exc:
        await museum_package.generate_sip()

    assert "Missing metadata value for key 'compression'" in str(exc.value)


@pytest.mark.asyncio
async def test_generate_sip_jpeg_mpo_not_supported(
        package_dir, museum_package_factory):
    """
    Test generating a SIP containing a MPO/JPEG file that
    is not supported
    """
    museum_package = await museum_package_factory("1234581")

    with pytest.raises(ValueError) as exc:
        await museum_package.generate_sip()

    assert "Missing metadata value for key 'compression'" in str(exc.value)


@pytest.mark.asyncio
async def test_generate_sip_with_non_ascii_filename(
        package_dir, mock_museumplus, museum_package_factory):
    """
    Test generating a SIP containing an attachment with a non-ASCII filename,
    which are not processed by the DPRES service yet
    """
    with pytest.raises(PreservationError) as exc:
        await museum_package_factory("1234582")

    assert "Filename contains non-ASCII characters" == exc.value.error


@pytest.mark.asyncio
async def test_generate_sip_multimedia_xml_not_allowed(
        package_dir, mock_museumplus, museum_package_factory):
    """
    Test generating a SIP containing an attachment named "Multimedia.xml",
    which is not allowed as it would overwrite a file with the same name
    """
    with pytest.raises(PreservationError) as exc:
        await museum_package_factory("1234583")

    assert (
        exc.value.error
        == "Filename 'Multimedia.xml' not allowed for attachment"
    )
