#!/bin/bash

# This script prompts the user for installation options and installs ParadeDB on their system. It
# is *not* intended for production use. The script is included in our website and can be curl'd.

set -Eeuo pipefail

########################################
# Variables
########################################

ARCH=$(uname -m)
LATEST_RELEASE_TAG=$(curl -s "https://api.github.com/repos/paradedb/paradedb/releases/latest" | jq -r .tag_name)
LATEST_RELEASE_VERSION="${LATEST_RELEASE_TAG#v}"


########################################
# Helper Functions
########################################

function commandExists() {
  command -v "$1" >/dev/null 2>&1
}

function isRHEL() {
  cat /etc/redhat-release >/dev/null 2>&1 
}

selectInstallationMethod() {
  local os_type=$1
  local binary_name=$2
  echo "On $os_type, ParadeDB is available via Docker or as prebuilt $binary_name Postgres extension binaries."

  OPTIONS=("Docker" "Extension Binary")
  select opt in "${OPTIONS[@]}"; do
    case $opt in
      "Docker")
        installDocker
        break
        ;;
      "Extension Binary")
        if [[ "$os_type" == "macOS" ]]; then
          installMacBinary
        else
          installBinary
        fi
        break
        ;;
      *)
        echo "Invalid option. Exiting..."
        break
        ;;
    esac
  done
}

# Determines linux base distro and installs docker accordingly
installDockerDepsLinux() {
  read -r -p "Would you like us to proceed with installing Docker on your system?\nIf you prefer to install Docker manually, please exit now and return once the installation is complete. [Y/n] " response
  case "$response" in
    [nN][oO]|[nN])
    exit 1;
  esac

  # Uses the docker installation script
  sh <(curl https://get.docker.com)
}


# Checks commandExists only once
# in the main loop to avoid the duplicate OS checking.
installDocker() {
  # Set default values
  pguser="myuser"
  pgpass="mypassword"
  dbname="paradedb"

  if ! commandExists docker; then
    if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
      echo -e "\nPlease install Docker first and get back to the setup!"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
      echo -e "Please install docker from: https://docs.docker.com/desktop/install/mac-install/ before proceeding with the installation."
      echo -e "$EXIT_MSG"
      exit 0
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
      installDockerDepsLinux
      sudo systemctl enable --now docker
      echo "Successfully Installed Docker ✅"
    else
      echo "Unsupported OS type: $OSTYPE"
      exit 1
    fi
  fi

  # Continue docker install
  docker_version=$(docker --version)
  echo "Docker version: $docker_version ..."
  # Check if Docker daemon is running
  if docker info >/dev/null 2>&1; then
    echo "Docker daemon is running. Pulling image..."
  else
    echo "Docker daemon is not running. Please run it to pull the ParadeDB image."
    exit 1
  fi

  # Prompt for user input
  read -r -p "Username for Database (default: myuser): " tmp_pguser
  if [[ -n "$tmp_pguser" ]]; then
    pguser="$tmp_pguser"
  fi

  read -r -p "Password for Database (default: mypassword): " tmp_pgpass
  if [[ -n "$tmp_pgpass" ]]; then
    pgpass="$tmp_pgpass"
  fi

  read -r -p "Name for your database (default: paradedb): " tmp_dbname
  if [[ -n "$tmp_dbname" ]]; then
    dbname="$tmp_dbname"
  fi

  if docker inspect "paradedb" > /dev/null 2>&1; then
    echo -e "We found a previous paradedb container on your system.\nWe need to remove it to continue this setup."
    read -r -p "Would you like to remove it? [y/N] " response
    case "$response" in
      [yY][eE][sS]|[yY])
        docker stop paradedb || true
        docker rm paradedb || true
        echo "Successfully Removed Container ✅"
    esac
  fi

  # Pull Docker image
  echo "Pulling Docker Image for Parade DB: docker pull paradedb/paradedb"
  docker pull paradedb/paradedb || { echo "Failed to pull Docker image"; exit 1; }
  echo -e "Pulled Successfully ✅\n"

  echo -e "Would you like to add a Docker volume to your database?\nA docker volume will ensure that your ParadeDB Postgres database is stored across Docker restarts.\nNote that you will need to manually update ParadeDB versions on your volume via: https://docs.paradedb.com/upgrading.\nIf you're only testing, we do not recommend adding a volume."

  volume_opts=("Yes" "No(Default)")

  select vopt in "${volume_opts[@]}"
  do
    case $vopt in
      "Yes")
        echo "Adding volume at: /var/lib/postgresql/data"
        docker run \
          --name paradedb \
          -e POSTGRES_USER="$pguser" \
          -e POSTGRES_PASSWORD="$pgpass" \
          -e POSTGRES_DB="$dbname" \
          -v paradedb_data:/var/lib/postgresql/data/ \
          -p 5432:5432 \
          -d \
          paradedb/paradedb:latest || { echo "Failed to start Docker container. Please check if an existing container is active or not."; exit 1; }
        break ;;
      *)
        docker run \
          --name paradedb \
          -e POSTGRES_USER="$pguser" \
          -e POSTGRES_PASSWORD="$pgpass" \
          -e POSTGRES_DB="$dbname" \
          -p 5432:5432 \
          -d \
          paradedb/paradedb:latest || { echo "Failed to start Docker container. Please check if an existing container is active or not."; exit 1; }
        break ;;
    esac
  done
  echo "Docker Container started ✅"

  # Provide usage information
  echo -e "\n\nTo use paradedb execute the command: docker exec -it paradedb psql $dbname -U $pguser"
}

# Installs Mac OS Binary
installMacBinary(){
  # Determine MacOS version
  OS_VERSION=$(sw_vers -productVersion)
  MAC_NAME=
  if [[ "$OS_VERSION" == 15.* ]]; then
    MAC_NAME="sequoia"
  elif [[ "$OS_VERSION" == 14.* ]]; then
    MAC_NAME="sonoma"
  else
    echo "Unsupported macOS version: $OS_VERSION"
    exit 1
  fi

  # Select postgres version
  pg_version=
  echo "Select postgres version[Please use 1 for v14, 2 for v15 and 3 for v16](Note: ParadeDB is supported on PG12-16. For other postgres versions, you will need to compile from source.)"
  versions=("14" "15" "16" "17")

  select vers in "${versions[@]}"
  do
    case $vers in
      "14")
        pg_version="14"
        break ;;
      "15")
        pg_version="15"
        break ;;
      "16")
        pg_version="16"
        break ;;
      "17")
        pg_version="17"
        break ;;
      *)
        echo "Invalid Choice! Please use 1 for v14, 2 for v15 and 3 for v16"
    esac
  done

  # Setup binary download URL
  filename="pg_search@${pg_version}--${LATEST_RELEASE_VERSION}.arm64_${MAC_NAME}.pkg"
  url="https://github.com/paradedb/paradedb/releases/download/${LATEST_RELEASE_VERSION}/"

  echo "Downloading ${url}"
  curl -l "$url" > "$filename" || false
  echo "Binary Downloaded Successfully!🚀"

  # Unpack PKG at the desired locations
  echo "Installing $filename..."
  sudo installer -pkg "$filename" -target /
  if [[ $? -ne 0 ]]; then
    echo "Installation failed. Please check the package and try again."
    exit 1
  fi

  echo "Installation completed successfully!"
}

installDeb(){
  # Install curl
  echo "Installing dependencies...."

  # Not required ----
  # echo "Installing cURL"
  #
  # sudo apt-get update -y || false
  # sudo apt-get install curl -y || false
  #
  # echo "Successfully Installed cURL✅"
  # -----------------

  # Confirm architecture
  if [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
  else
    ARCH="arm64"
  fi

  # Sets the variable DEB_DISTRO_NAME according to release
  DEB_DISTRO_NAME=$(awk -F'[= ]' '/VERSION_CODENAME=/{print $2}'  /etc/os-release)

  filename="postgresql-$1-pg-search_${LATEST_RELEASE_VERSION}-1PARADEDB-${DEB_DISTRO_NAME}_${ARCH}.deb"
  url="https://github.com/paradedb/paradedb/releases/latest/download/${filename}"

  echo "Downloading ${url}"

  curl -L "$url" > "$filename" || false

  sudo apt install ./"$filename" -y || false
}

# Installs latest RPM package
# Supports EL8 and EL9, asks the user and have them install the right one
installRPM(){

  # Not required ---------------------
  # echo -e "Installing cURL"
  # sudo dnf install curl || false
  # echo "Successfully Installed cURL✅"
  # ----------------------------------

  # gives version number like 8 or 9
  rhel_version=$(awk -F'[. ]' '/release/{print $6}' /etc/redhat-release)

  # Confirm architecture
  if [ "$ARCH" != "x86_64" ]; then
    ARCH="aarch64"
  fi

  filename="pg_search_$1-$LATEST_RELEASE_VERSION-1PARADEDB.el${rhel_version}.${ARCH}.rpm"
  url="https://github.com/paradedb/paradedb/releases/latest/download/${filename}"


  echo "Downloading ${url}"
  curl -l "$url" > "$filename" || false

  sudo rpm -i "$filename" || false
  echo "ParadeDB installed successfully!"
}


# Installs latest binary for ParadeDB
installBinary(){

  # Select postgres version
  pg_version=
  echo "Select postgres version[Please use 1 for v14, 2 for v15 and 3 for v16](Note: ParadeDB is supported on PG12-16. For other postgres versions, you will need to compile from source.)"
  versions=("14" "15" "16" "17")

  select vers in "${versions[@]}"
  do
    case $vers in
      "14")
        pg_version="14"
        break ;;
      "15")
        pg_version="15"
        break ;;
      "16")
        pg_version="16"
        break ;;
      "17")
        pg_version="17"
        break ;;
      *)
        echo "Invalid Choice! Please use 1 for v14, 2 for v15 and 3 for v16"
    esac
  done

  # Select Base type
  if isRHEL; then
    echo -e "ON RHEL"
    installRPM $pg_version
    # exit 1
  else
    installDeb $pg_version
  fi
}








########################################
# Main Loop
########################################

echo -e ""
echo -e "  _____                    _      _____  ____     "
echo -e " |  __ \                  | |    |  __ \|  _ \    "
echo -e " | |__) |_ _ _ __ __ _  __| | ___| |  | | |_) |   "
echo -e " |  ___/ _\` | '__/ _\` |/ _\` |/ _ \ |  | |  _ < "
echo -e " | |  | (_| | | | (_| | (_| |  __/ |__| | |_) |   "
echo -e " |_|   \__,_|_|  \__,_|\__,_|\___|_____/|____/    "                                     
echo -e ""
echo -e ""
echo -e "🚀 Welcome to ParadeDB Installation Script 🚀"
echo -e ""
echo -e "ParadeDB is an alternative to Elasticsearch build as a Postgres extension."
echo -e "It is available as prebuilt binaries for installing in a self-hosted Postgres instance, as a Docker image, and as a Helm chart for deployment on Kubernetes."
echo -e ""
echo -e "To deploy on Kubernetes, please refer to our documentation: https://docs.paradedb.com/deploy/overview"
echo -e "Otherwise, please select an installation method below."
echo -e ""

# On Windows, only Docker is supported
if [[ "$OSTYPE" = "msys" ]] || [[ "$OSTYPE" = "cygwin" ]]; then
  echo "Operating system detected: Windows"
  read -r -p "Please note that only Docker is supported on Windows. Continue with the Docker setup? [y/N]" response
  case "$response" in
    [yY][eE][sS]|[yY])
      installDocker
  esac
# On macOS, both Docker and prebuilt binaries are supported
elif [[ "$OSTYPE" = "darwin"* ]]; then
  echo "Operating system detected: macOS"
  selectInstallationMethod "macOS" ".dylib"
# On Linux, both Docker and prebuilt binaries are supported
else
  # Detect the sub-Linux version -> Added isRHEL to determine if base system is RHEL based
  echo "Operating system detected: Linux"
  selectInstallationMethod "Linux" ".deb or .rpm"
fi

# Exit message
echo -e ""
echo -e "✅ Installation complete."
echo -e ""
echo -e "Thank you for installing ParadeDB! 🎉"
echo -e "To get started, please refer to our quickstart tutorial: https://docs.paradedb.com/documentation/getting-started/quickstart"
echo -e "To stay up to date and get help, please join our Slack community: https://join.slack.com/t/paradedbcommunity/shared_invite/zt-2lkzdsetw-OiIgbyFeiibd1DG~6wFgTQ"
echo -e "And don't forget to star us on GitHub: https://github.com/paradedb/paradedb"
echo -e ""
