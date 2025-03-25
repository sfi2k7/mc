# MongoDB Collection Transfer Utility (mc)

A utility for transferring MongoDB collections between servers. It exports collections to a compressed binary format that preserves BSON types and can be imported back to any MongoDB instance.

## Features

- Export MongoDB collections to compressed binary files
- Import collections from these files to any MongoDB instance
- Preserves all BSON types (ObjectIDs, dates, etc.)
- Memory-efficient processing of large collections
- Simple command-line interface

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mc.git
cd mc

# Build the binary
go build -o mc

# Optionally, install to your PATH
mv mc /usr/local/bin/