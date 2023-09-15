#!/bin/bash
set -e  # Stop if there is a failure

# setup_datasets.sh
# A file to setup the datasets

#------------- DOWNLOAD UTILITY -------------#
function download_dataset() {
    FILE=$1;
    data_dir=$2;
    URL=$3;
    CHECKSUM=$4;
   
    # Check if file already exists
    if [ -f "${data_dir}/${FILE}" ]; then
    echo "file ${FILE} exists, checking hash..."
        # Exists -> check the checksum
        sha_result=$(sha256sum "${data_dir}/${FILE}" | awk '{ print $1 }')
        if [ "${sha_result}" != "${CHECKSUM}" ]; then
            echo "wrong checksum, retrying..."
            echo "EXPECTED ${CHECKSUM}"
            echo "GOT      ${sha_result}"
            rm "${data_dir}/${FILE}"
            curl -L $URL | zstd -d > "${data_dir}/${FILE}"
        fi
    else
        # Download
        curl -L $URL | zstd -d > "${data_dir}/${FILE}"
    fi

    # Validate (at this point the file should really exist)
    count=0
    ok=0
    while [ $count -lt 10 ]
    do
        sha_result=$(sha256sum "${data_dir}/${FILE}" | awk '{ print $1 }')
        if [ "${sha_result}" == "${CHECKSUM}" ]; then
            echo -e ${FILE} "checksum ok\n"
            ok=1
            break
        fi
        rm "${data_dir}/${FILE}"
        echo "wrong checksum, retrying..."
        echo "EXPECTED ${CHECKSUM}"
        echo "GOT      ${sha_result}"
        curl -L $URL | zstd -d > "${data_dir}/${FILE}"
        count=$((count+1))
    done
    if [ $ok -eq 0 ]; then
        echo "download has failed 10 times. please, run again."
        exit -1
    fi
}

# Checksums
check_fb="22d5fd6f608e528c2ab60b77d4592efa5765516b75a75350f564feb85d573415"
check_wiki="097f218d6fc55d93ac3b5bdafc6f35bb34f027972334e929faea3da8198ea34d"
#check_books="6e690b658db793ca77c1285c42ad681583374f1d11eb7a408e30e16ca0e450da"
check_osm="1d1f5681cbfcd3774de112533dccc4065b58b74adf09684e9ad47298d1caa9e0"

# URLs
url_fb="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7"
url_wiki="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/SVN8PI"
#url_books="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/5YTV8K"
url_osm="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/8FX9BV"

#--------------------------------------------#

INITIAL_DIR=$(pwd)
_source_dir_=$(dirname "$0")
BASE_DIR=$(readlink -f "${_source_dir_}/..")     # /home/ilaria/Documents/stage/hashing-benchmark-docker

# Check if the user has provided an argument
if [ $# -eq 0 ]; then
    echo "Using default directory: hashing-benchmark-docker/data"
    data_dir="${BASE_DIR}/data"
else
    data_dir=$1
fi

data_dir=$(realpath $data_dir)

mkdir -p $data_dir

# Check if datasets are there
download_dataset "fb_200M_uint64" $data_dir $url_fb $check_fb
download_dataset "wiki_ts_200M_uint64" $data_dir $url_wiki $check_wiki
#download_dataset "books_200M_uint32" $data_dir $url_books $check_books
download_dataset "osm_cellids_200M_uint64" $data_dir $url_osm $check_osm

# Patch the /src/support/datasets.hpp file (-i = in place)
# if [ $# -lt 2 ] || [ "$2" != "--no-patch" ]; then
#     sed -i "s,../data,${data_dir},g" ${BASE_DIR}/code/src/support/datasets.hpp
# fi