#!/bin/bash

unix_installation() {
    curl -L "$EXECUTABLE" > "$INSTALL_DIR"

    chmod a+x "$INSTALL_DIR"

    if ! grep -q "$INSTALL_DIR" ~/.bash_profile; then
        echo 'export PATH="$PATH:/usr/local/bin"' >> ~/.bash_profile
        source ~/.bash_profile
    fi
}

windows_installation() {
    curl -L "$EXECUTABLE" > "$INSTALL_DIR"

    if ! grep -q "$INSTALL_DIR" ~/.bash_profile; then
        echo 'export PATH="$PATH:/usr/local/bin"' >> ~/.bash_profile
        source ~/.bash_profile
    fi
}

INSTALL_DIR="/usr/local/bin/doclite"
EXECUTABLE="https://github.com/humblewolfstudio/doc-lite/releases/download/v0.1/doclite"

os_name=$(uname)

# Check the operating system
if [[ "$os_name" == "Linux" ]]; then
    unix_installation
elif [[ "$os_name" == "Darwin" ]]; then
    unix_installation
elif [[ "$os_name" == "CYGWIN"* || "$os_name" == "MINGW32"* || "$os_name" == "MSYS"* ]]; then
    windows_installation
else
    echo "Unknown operating system"
fi

echo "Installation complete. doclite is now in your PATH."