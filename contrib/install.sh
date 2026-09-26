#!/bin/sh
# Install or upgrade MyDHT as a systemd service.
#
#   sudo ./contrib/install.sh
#
# Installs the package into a virtualenv in /opt/mydht, the unit files into
# /etc/systemd/system and, unless they already exist, settings files in
# /etc/default/mydht and /etc/default/mydht-sync. Run it again after
# pulling new code to upgrade. The optional file sync (mydht-sync.timer)
# is installed but not enabled.
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

install -m 755 "$src/contrib/mydht-sync" /opt/mydht/bin/mydht-sync
for unit in mydht.service mydht-sync.service mydht-sync.timer; do
    install -m 644 "$src/contrib/$unit" "/etc/systemd/system/$unit"
done
if [ ! -e /etc/default/mydht ]; then
    install -m 644 "$src/contrib/mydht.env" /etc/default/mydht
    fresh=1
fi
if [ ! -e /etc/default/mydht-sync ]; then
    install -m 644 "$src/contrib/mydht-sync.env" /etc/default/mydht-sync
fi
systemctl daemon-reload

if [ "${fresh:-0}" = 1 ]; then
    echo "Installed. Edit /etc/default/mydht, then run:"
    echo "  systemctl enable --now mydht"
    echo "To keep a file in sync, edit /etc/default/mydht-sync and run:"
    echo "  systemctl enable --now mydht-sync.timer"
else
    echo "Upgraded. Restart to use the new version:"
    echo "  systemctl restart mydht"
fi
