#!/bin/bash
find . -maxdepth 1 -type f -name '*php' -not -name 'application.php' | xargs rm
