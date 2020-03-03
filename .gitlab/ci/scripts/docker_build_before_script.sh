apk add git
export BASE_SHA=$(git hash-object requirements.txt)
echo Building permifrost on $BASE_SHA
