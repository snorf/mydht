#!/bin/sh
# Install or upgrade MyDHT as a systemd service.
#
#   sudo ./contrib/install.sh
#
# Installs the package into a virtualenv in /opt/mydht, the unit file into
# /etc/systemd/system and, unless it already exists, a settings file in
# /etc/default/mydht. Run it again after pulling new code to upgrade.
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run as root (sudo $0)" >&2
    exit 1
fi

src=$(cd "$(dirname "$0")/.." && pwd)

if ! python3 -c "import venv, ensurepip" 2>/dev/null; then
    echo "python3 with venv is required (Debian/Ubuntu: apt install python3-venv)" >&2
    exit 1
fi

[ -x /opt/mydht/bin/python ] || python3 -m venv /opt/mydht
/opt/mydht/bin/pip install --quiet --upgrade "$src"

install -m 644 "$src/contrib/mydht.service" /etc/systemd/system/mydht.service
if [ ! -e /etc/default/mydht ]; then
    install -m 644 "$src/contrib/mydht.env" /etc/default/mydht
    fresh=1
fi
systemctl daemon-reload

if [ "${fresh:-0}" = 1 ]; then
    echo "Installed. Edit /etc/default/mydht, then run:"
    echo "  systemctl enable --now mydht"
else
    echo "Upgraded. Restart to use the new version:"
    echo "  systemctl restart mydht"
fi
