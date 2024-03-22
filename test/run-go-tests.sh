#!/bin/bash -e

# TODO: add coverage report (atw)

# directories to ignore because they have no tests or are not go packages
badDirectories="bin configuration custom Docker docs examples test test-integration test-integration-docker"

function exists_in_list() {
    LIST=$badDirectories
    DELIMITER=" "
    VALUE=$1 
    LIST_WHITESPACES=`echo $LIST | tr "$DELIMITER" " "`
    for x in $LIST_WHITESPACES; do
        if [ "$x" = "$VALUE" ]; then
            return 0
        fi
    done
    return 1
}

# walk the directory and run go test for each directory, except for test and test-integration
for dir in ./*/     # list directories in the form "/dirname/"
do
    dir=${dir%*/}      # remove the trailing "/"
    echo "${dir##*/}"    # print everything after the final "/"
    # check if the directory is in the list of bad directories
    # echo exists_in_list "${dir##*/}"
    if exists_in_list "${dir##*/}"; then
        # echo "one of the badDirectories"
        continue
    else
        # echo "start test for ${dir##*/}"
         cd $dir
        go test
        # check if the test failed
        if [ $? -ne 0 ]; then
            cd ..
            exit 1
        fi
        cd ..
    fi

done


