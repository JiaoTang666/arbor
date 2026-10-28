import io
import tarfile
import pytest
from arbor.download import extract
from arbor.download import fetch
from arbor.io import save, sha


def test_archive_links_and_traversal_are_rejected(tmp_path):
    for name,kind in (("../escape",tarfile.REGTYPE),("link",tarfile.SYMTYPE)):
        archive = tmp_path/"bad.tgz"
        with tarfile.open(archive,"w:gz") as f:
            item=tarfile.TarInfo(name); item.type=kind; item.linkname="/outside"
            f.addfile(item,io.BytesIO())
        with pytest.raises(ValueError,match="unsafe path"):
            extract(archive,tmp_path/"out")


def test_public_asset_works_without_cli_even_for_private_preview_manifest(tmp_path, monkeypatch):
    import arbor.download as download
    dataset = tmp_path/"dataset"
    dataset.mkdir()
    save(dataset/"manifest.json", dict(files={}))
    archive = tmp_path/"data.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        stream.add(dataset/"manifest.json", arcname="manifest.json")
    manifest = tmp_path/"download.json"
    save(manifest, dict(asset="data.tar.gz", url="https://example.test/data.tar.gz", private=True,
         sha256=sha(archive), dataset_manifest_sha256=sha(dataset/"manifest.json"), required_free_bytes=0))
    monkeypatch.setattr(download.urllib.request, "urlopen", lambda *_ , **kw: io.BytesIO(archive.read_bytes()))
    monkeypatch.setattr(download.shutil, "which", lambda _: None)
    fetch(manifest, tmp_path/"installed")
    assert (tmp_path/"installed/manifest.json").exists()
