#!/bin/bash

# Define variables
INSTALL_DIR="/usr/local/bin/doclite"
EXECUTABLE="https://github.com/humblewolfstudio/doc-lite/releases/download/v0.1/doclite"

# Copy the executable to the installation directory
curl -L "$EXECUTABLE" > "$INSTALL_DIR"

chmod a+x "$INSTALL_DIR"

if ! grep -q "$INSTALL_DIR" ~/.bash_profile; then
    # Add the installation directory to the PATH in the user's shell profile
    echo 'export PATH="$PATH:/usr/local/bin"' >> ~/.bash_profile
    # Source the user's shell profile to apply the changes
    source ~/.bash_profile
fi

echo "Installation complete. $EXECUTABLE is now in your PATH."
