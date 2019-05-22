#!/bin/bash
#
# Basic AXL sanity test.
#
# Make some files in /tmp and copy them using AXL.
#

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

	# Create a directory structure
	dirs=(./ home project/dir1 project/dir2 docs/c1/data docs/c2/data docs/c2)
	for i in "${dirs[@]}" ; do
		mkdir -p "$src/$i"
	done
	num_dirs=${#dirs[@]}

	# Make 50 files and put them in our directories
	for i in {1..50} ; do
		dirnum=$(($i % $num_dirs))
		tmp="$src/${dirs[$dirnum]}"
		dd if=/dev/zero of="$tmp/$i.file" bs=1k count=$i &>/dev/null
	done
}

function run_test
{
	xfer=$1
	rc=0;
	echo -en "Testing $xfer transfer...\t"
	if ! out1="$(./axl_cp -X $xfer -r $src/* $dest)" ; then
		echo "failed copy"
		echo "$out1"
		rc=1
	else
		# Compare the two directories
		if ! out2="$(diff -qr $src $dest)" ; then
			echo "failed diff"
			echo "$out1"
			echo "---"
			echo "$out2"
			rc=2
		else
			echo "success"
		fi
	fi
	rm -fr "$dest"/*
	if [ "$rc" != "0" ] ; then
		false
	else
		true
	fi
}

create_files

# Run our tests
xfers="sync pthread"
for i in $xfers ; do
	if ! run_test $i ; then
		cleanup
		exit 1
	fi
done
cleanup
