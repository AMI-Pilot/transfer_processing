# AMI Transfer Processing
When an AV package arrives in the dropbox these tools will:
* Verify the checksums for the files
* Generate derivatives for the content and create a metadata file which will be imported into Avalon Media System
* Distribute the derivatives to IU's on-site S3 server and send the metadata into Avalon for ingestion
* Copy the original files and derivatives to IU's Scholarly Data Archive

The state machine for this system is documented in the docs directory and looks something like this:

![State Machine](https://github.com/AMI-Pilot/transfer_processing/raw/main/docs/object_states.dot.svg)