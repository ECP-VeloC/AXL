#!/bin/bash
#
# Basic AXL sanity test.
#
# Make some files in /tmp and copy them using AXL.
#

function usage
{
echo "
 Usage: test_axl [-c sec [-k]] [-n num_files] [xfer_type]

   -c sec:		Cancel transfer after 'sec' seconds (can be decimal number)
   -n num_files:	Number of files to create (default 50)
   xfer_type:		sync|pthread|bbapi|dw (defaults to sync if none specified)
"
}

function isnum
{
	[[ "$1" =~ ^[0-9.]+$ ]]
}

while getopts "c:kn:" opt; do
	case "${opt}" in
	c)
		sec=${OPTARG}
		if ! isnum $sec ; then
			echo "'$sec' is not a number of seconds"
			usage
			exit 1
		fi
		;;
	n)
		num_files=${OPTARG}
		if ! isnum $num_files ; then
			echo "'$num_files' is not a number"
			usage
			exit
		fi
		;;

	*)
		usage
		exit
	esac
done
shift $((OPTIND-1))

xfer=$1
if [ -z "$xfer" ] ; then
    xfer=sync
fi

case $xfer in
	sync) ;;
	pthread) ;;
	bbapi) ;;
	dw) ;;
	*)
		echo "Invalid transfer type '$xfer'"
		exit 1;
	;;
esac

src=$(mktemp -d)
dest=$(mktemp -d)

trap ctrl_c INT

function cleanup
{
	rm -fr "$src" "$dest"
}

function ctrl_c() {
        cleanup
}

function create_files {
	num=$1
	if [ -z "$num" ] ; then
		# default to creating 50 files
		num=50
	fi
	# Create a directory structure
	dirs=(./ home project/dir1 project/dir2 docs/c1/data docs/c2/data docs/c2)
	for i in "${dirs[@]}" ; do
		mkdir -p "$src/$i"
	done
	num_dirs=${#dirs[@]}

	# Make 50 files and put them in our directories
	for ((i=0; i < $num; i++)) ; do
		dirnum=$(($i % $num_dirs))
		tmp="$src/${dirs[$dirnum]}"
		dd if=/dev/zero of="$tmp/$i.file" bs=1k count=$i &>/dev/null
	done
}

# $1 Transfer type
# $2 Timeout in seconds (optional).  This is needed for the AXL_Cancel tests.
function run_test
{
	xfer=$1
	s=$2

	if [ -z "$s" ] ; then
		# No timeout specified, just make it super long
		s=9999
	fi
	rc=0;
	out1="$(timeout --preserve-status $s ./axl_cp -X $xfer -r $src/* $dest)"
	rc=$?
	if [ "$rc" != "0" ] ; then
		echo "failed copy, rc=$rc"
		echo "$out1"
		rc=1
	fi

	if [ "$rc" != "0" ] ; then
		false
	else
		true
	fi
}

create_files $num_files

# Run our tests
if [ -z "$sec" ] ; then
	echo -en "Testing $xfer transfer...\t"
else
	echo -en "Testing $xfer transfer cancel after $sec seconds...\t"
fi

# There are two types of tests we do here.
#
# 1. A basic copy
# 2. A basic copy where we cancel it partway though to test AXL_Cancel
#
# First run our test, and optionally cancel it
if ! run_test $xfer $sec ; then
	# Our copy failed for some reason (independent of the cancellation)
	cleanup
	exit 1
else
	# Files are copied, verify they're all there and correct
	if ! out2="$(diff -qr $src $dest)" ; then
		# Files aren't all there.  If we canceled the transfer this is
		# good, since they shouldn't be all there.  Otherwise they
		# should be there.
		if [ -n "$sec" ] ; then
			echo "success"
		else
			echo "failed. transfer output was:"
			echo "$out1"
			echo "--- diff was ---"
			echo "$out2"
			cleanup
			exit 1
		fi

	else
		if [ -n "$sec" ] ; then
			echo "failure.  Files were all copied before the cancel"
			cleanup
			exit 1
		else
			echo "success"
		fi
	fi
fi

cleanup
