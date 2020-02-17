apk add git
export BASE_SHA=$(git hash-object requirements.txt)
echo Building meltano-permissions on $BASE_SHA