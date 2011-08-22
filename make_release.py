import shutil
import glob
import os
import tarfile

version = "1.0"
os.system("sbt package")

if os.path.exists("dist/lib"):
    shutil.rmtree("dist/lib")
shutil.copytree("lib", "dist/lib")
shutil.copy("target/scala_2.9.0/deduptr_2.9.0-%s.jar" % version, "dist/dedup_tr.jar")

release_tar = tarfile.TarFile("dedup_tr-%s.tgz" % version, mode="w")
release_tar.add("dist", "dedup_tr")
