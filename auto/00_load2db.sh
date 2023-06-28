#! /bin/bash
workDir="/c/Users/datamicron/Documents/vscode/house_pricing"
srcDir="/c/Users/datamicron/Documents/vscode/house_pricing/src"
autoDir="/c/Users/datamicron/Documents/vscode/house_pricing/src"

cd "$workDir" || exit
rm -rf logs/*
cd "$srcDir" || exit
echo -e "------- STAGING PROCESS STARTS HERE -------"

paste -d " " <(seq 1 1 5) <(ls 0*.py | grep -v "00") | while read -r a b
do
    py "$b"
    echo -e "\n"
done

echo -e "------- STAGING PROCESS ENDS HERE -------"
