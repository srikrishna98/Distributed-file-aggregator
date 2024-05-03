import uuid
import uvicorn

from JobManager import JobManager
from fastapi import FastAPI, BackgroundTasks # type: ignore
from fastapi.encoders import jsonable_encoder # type: ignore
from fastapi.responses import JSONResponse # type: ignore

from DTO.FileMetadataDTO import FileMetadataDTO
from DTO.JobDTO import Job

from FileGenerator import FileGenerator


app = FastAPI()

    
@app.get("/")
async def root():
    return {"message": "Generator File System is Up!"}


@app.post("/generateFiles")
async def generateFiles(fileMetadata: FileMetadataDTO, background_tasks: BackgroundTasks):
    job = Job(jobId = str(uuid.uuid4()).replace("-", ""))
    
    background_tasks.add_task(FileGenerator.createFiles,fileMetadata, job.jobId)
    background_tasks.add_task(JobManager.createJobs, fileMetadata, job.jobId)
    return JSONResponse(content=jsonable_encoder(job))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")