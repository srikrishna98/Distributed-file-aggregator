from pydantic import BaseModel

class FileMetadataDTO(BaseModel):
    fileCount: int
    numberCount: int