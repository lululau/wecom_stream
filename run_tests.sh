#!/bin/bash
# Run wecom_stream plugin tests
# Usage: cd ~/.hermes/plugins/wecom_stream && ./run_tests.sh

cd "$(dirname "$0")"
PYTHONPATH="$HOME/.hermes/hermes-agent:." .venv/bin/python -m pytest tests/ -v "$@"
