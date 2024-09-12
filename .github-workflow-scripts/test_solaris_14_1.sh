#!/usr/bin/env sh
#
# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To better debug these steps run a tmp solaris VM like so: https://github.com/vmactions/shell-solaris
set -e # Exit immediately if a cmd returns a non-zero status
id -u -n
pkg install sudo pv || true
pkgutil -y -i zstd mbuffer netcat
uname -a
uname -v
if [ "$(uname -v)" = "11.4.0.15.0" ]; then
  pyversion=3.9.20; export ax_cv_c_float_words_bigendian=no
  # pyversion=3.8.19; export ax_cv_c_float_words_bigendian=no
  # pyversion=3.7.17 # works fine too
  # pyversion=3.10.14; export ax_cv_c_float_words_bigendian=no # works fine too
  wget https://www.python.org/ftp/python/$pyversion/Python-$pyversion.tgz
  tar -zxf Python-$pyversion.tgz
  cd Python-$pyversion
  pkg install gcc || true
  ./configure --prefix=/python3  # --enable-optimizations
  make
  rm -fr /python3
  make install
  cd ..
else  # it's a more recent solaris version and as such has python >= 3.7 preinstalled
  if false; then  # 3.7 would work fine too
    pkg install coverage-37  # for coverage.sh only
  else
    pkg install runtime/python-39
    pkg install coverage-39  # for coverage.sh only
    mkdir -p /python3/bin
    ln -s $(command -v python3.9) /python3/bin/python3
  fi
  ##wget https://bootstrap.pypa.io/pip/3.7/get-pip.py  # see https://github.com/pypa/get-pip
  ##sudo python3 get-pip.py  # for coverage.sh only: manually install pip as it is missing in the preinstalled version
fi
id -u -n
uname -a
zfs help
zfs help list
python3 --version
export PATH=/python3/bin:$PATH
python3 --version
ssh -V
zstd --version
pv --version | head -n 1
mbuffer --version |& head -n 1
command -v sh | xargs ls -l
df -h /tmp

mkdir -p $HOME/.ssh
rm -f $HOME/.ssh/id_rsa $HOME/.ssh/id_rsa.pub
ssh-keygen -t rsa -f $HOME/.ssh/id_rsa -q -N ""  # create private key and public key
cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
ls -al $HOME

echo "Now running coverage"; ./coverage.sh
echo "Now running tests as root user"; ./test.sh

echo "Now running tests as non-root user:"
tuser=test
thome=/home/$tuser
userdel -r $tuser || true
useradd -d $thome -m $tuser
echo "$tuser ALL=NOPASSWD:$(command -v zfs),$(command -v zpool)" >> /etc/sudoers
supath=$(grep "^SUPATH=" /etc/default/login | cut -d'=' -f2)
echo "PATH=$supath" >> /etc/default/login  # ensure zstd, mbuffer, etc are on the PATH via ssh

mkdir -p $thome/.ssh
cp -p $HOME/.ssh/id_rsa $HOME/.ssh/id_rsa.pub $thome/.ssh/
chown -R $tuser $thome/.ssh
cat $HOME/.ssh/id_rsa.pub | sudo -u $tuser tee -a $thome/.ssh/authorized_keys > /dev/null
chmod go-rwx $thome/.ssh/authorized_keys

cp -rp . $thome/wbackup-zfs # if running with Github Action
# cp -rp wbackup-zfs $thome/ # if running with https://github.com/vmactions/shell-solaris
chown -R $tuser $thome/wbackup-zfs
sudo -u $tuser sh -c 'export PATH=/python3/bin:$PATH; '"cd $thome/wbackup-zfs; ./test.sh"
echo "wbackup-zfs-testrun-success"

#pkg update --accept; reboot # FIXME: reboot disconnects
#pkg search python
