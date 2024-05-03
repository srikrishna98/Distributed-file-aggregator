import os
import random
import io
import asyncio
from DTO.FileMetadataDTO import FileMetadataDTO


class FileGenerator:
    def _createFileContent(self, numberCount: int, fileName: str, jobId: str):
        numberRange = range(1)
        fileContent = [random.choice(numberRange) for _ in range(numberCount)]
        print(fileContent)
        if(not (os.path.exists(os.path.join(os.getcwd(), f"Data/{jobId}")))):
            os.makedirs(os.path.join(os.getcwd(), f"Data/{jobId}"))
        filePath=os.path.relpath(f"Data/{jobId}",os.getcwd())
        with io.open(f"{filePath}/{fileName}", "w") as file:
            file.write("[" + (','.join(str(content) for content in fileContent)) + "]")
        file.close()


    async def _createFile(self, fileMetadata: FileMetadataDTO, jobId: str, i: int):
        fileName = f"{jobId}_file_{i}.txt"
        self._createFileContent(fileMetadata.numberCount, fileName, jobId)

    async def createFiles(self, fileMetadata: FileMetadataDTO, jobId: str):
        tasks = []
        for i in range(1, fileMetadata.fileCount + 1):
            task = asyncio.create_task(self._createFile(fileMetadata, jobId, i))
            tasks.append(task)
        await asyncio.gather(*tasks)
