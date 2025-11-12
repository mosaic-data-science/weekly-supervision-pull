#!/bin/bash
# Script to update git remote after repository rename

echo "Updating git remote URL from weekly-supervision-pull to daily-supervision-pull..."
echo ""

# Update the remote URL
git remote set-url origin git@github.com:mosaic-data-science/daily-supervision-pull.git

# Verify the change
echo "Updated remote URL:"
git remote -v

echo ""
echo "✓ Remote URL updated successfully!"
echo ""
echo "Note: If you haven't renamed the repository on GitHub yet, please do that first:"
echo "1. Go to: https://github.com/mosaic-data-science/daily-supervision-pull/settings"
echo "2. Scroll to 'Repository name' section"
echo "3. Change name to 'daily-supervision-pull'"
echo "4. Click 'Rename'"

