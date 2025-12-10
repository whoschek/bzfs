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

set -e # Exit immediately if a cmd returns a non-zero status
pkg install -y python3 sudo zstd pv mbuffer bash netcat devel/py-coverage
id -u -n
uname -a
zfs --version || true
python3 --version
ssh -V
zstd --version
pv --version | head -n 1
mbuffer --version | head -n 1
command -v sh | xargs ls -l
sudo command -v sh | xargs ls -l

mkdir -p $HOME/.ssh
rm -f $HOME/.ssh/testid_rsa $HOME/.ssh/testid_rsa.pub
ssh-keygen -t rsa -f $HOME/.ssh/testid_rsa -q -N ""  # create private key and public key
cat $HOME/.ssh/testid_rsa.pub >> $HOME/.ssh/authorized_keys
ls -al $HOME $HOME/.ssh/testid_rsa

# change shell as default shell on freebsd <= 13 is csh instead of sh
cat /etc/shells
my_user=root
echo "Default shell of $my_user before change:"
getent passwd $my_user | cut -d: -f7
echo "Setting the default shell of $my_user to sh because csh quoting of special characters is not compatible with bzfs ..."
chsh -s /bin/sh $my_user
echo "Default shell of $my_user after change:"
getent passwd $my_user | cut -d: -f7
export SHELL=/bin/sh

echo "Using bzfs_test_mode=$bzfs_test_mode"
echo "Now running tests as root user"; ./test.sh
echo "Now running coverage"; ./coverage.sh

echo "Now running tests as non-root user:"
tuser=test
thome=/home/$tuser
pw userdel -n $tuser || true
pw useradd $tuser -d $thome -m
echo "$tuser ALL=NOPASSWD:$(command -v zfs),$(command -v zpool),$(command -v dd)" >> /usr/local/etc/sudoers

mkdir -p $thome/.ssh
cp -p $HOME/.ssh/testid_rsa $HOME/.ssh/testid_rsa.pub $HOME/.ssh/authorized_keys $thome/.ssh/
chmod go-rwx "$thome/.ssh/authorized_keys"
chown -R "$tuser" "$thome/.ssh"
cp -R . "$thome/bzfs"
chown -R "$tuser" "$thome/bzfs"

# change shell as default shell on freebsd <= 13 is csh instead of sh
my_user=$tuser
echo "Default shell of $my_user before change:"
getent passwd $my_user | cut -d: -f7
echo "Setting the default shell of $my_user to sh because csh quoting of special characters is not compatible with bzfs ..."
chsh -s /bin/sh $my_user
echo "Default shell of $my_user after change:"
getent passwd $my_user | cut -d: -f7

sudo -u $tuser sh -c "cd $thome/bzfs; bzfs_test_mode=$bzfs_test_mode bzfs_test_no_run_quietly=$bzfs_test_no_run_quietly ./test.sh"
echo "bzfs-testrun-success"
