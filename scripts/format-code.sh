#!/bin/bash

# Check if clang-format is installed
if ! command -v clang-format &> /dev/null; then
    >&2 echo "[ERROR] clang-format is not installed"
    echo "[INFO] You can install with 'sudo apt install clang-format'"

    exit 1;
fi

FOUND_FILES=0
FORMATTED_FILES=0

directories=("source")

echo "[INFO] Running clang-format on specified directories: ${directories[*]}"

for dir in "${directories[@]}"; do
    echo "[INFO] Processing directory: $dir"

    while IFS= read -r file; do
        RAW_HASH=$(sha256sum "$file")

        clang-format --Wno-error=unknown -i "$file" &> /dev/null
        FORMATTED_HASH=$(sha256sum "$file")

        if [ "$RAW_HASH" != "$FORMATTED_HASH" ]; then
            if ! git check-ignore -q "$file"; then
                FORMATTED_FILES=$((FORMATTED_FILES + 1))
                echo "[INFO] Formatted: $file"
            fi
        fi

        FOUND_FILES=$((FOUND_FILES + 1))
    done < <(find "$dir" -type f \( -name "*.cpp" -o -name "*.hpp" -o -name "*.h" -o -name "*.c" \))
done

if [ "${FOUND_FILES}" -eq 0 ]; then
    echo "[ERROR] No files found to format!"

    exit 1
fi

if [ "${FORMATTED_FILES}" -eq 0 ]; then
    echo "[INFO] Found ${FOUND_FILES} files, all files were already formatted!"

    exit 0;
else
    echo "[INFO] Found ${FOUND_FILES} files, formatted ${FORMATTED_FILES} file(s)"

    exit 0;
fi
