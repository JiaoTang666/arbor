import io
import tarfile
import pytest
from arbor.download import extract


def test_archive_links_and_traversal_are_rejected(tmp_path):
    for name,kind in (("../escape",tarfile.REGTYPE),("link",tarfile.SYMTYPE)):
        archive = tmp_path/"bad.tgz"
        with tarfile.open(archive,"w:gz") as f:
            item=tarfile.TarInfo(name); item.type=kind; item.linkname="/outside"
            f.addfile(item,io.BytesIO())
        with pytest.raises(ValueError,match="unsafe path"):
            extract(archive,tmp_path/"out")
