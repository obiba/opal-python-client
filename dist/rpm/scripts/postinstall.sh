#!/bin/sh
set -e

# link opal folder to default python lib, /usr/lib/python3/dist-packages may not be icluded in the lib path
python2_lib=/usr/lib/python2.7/dist-packages

case "$1" in
  0)
    if [ -d $python2_lib ]; then
      rm -f $python2_lib/opal
    fi
  ;;

  [1-2])
    if [ -d $python2_lib ]; then
      rm -f $python2_lib/opal
    fi
  ;;

  *)
    echo "postinst called with unknown argument \`$1'" >&2
    exit 1
  ;;
esac
exit 0
