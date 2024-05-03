import os

class Compute:

    def __init__(self, inputFiles:list, outputFile:str, isAverage:bool, elementCount: int, root_job_id:str):
        self.root_job_id = root_job_id
        self.inputFiles = inputFiles
        self.outputFile = outputFile
        self.isAverage = isAverage
        self.elementCount = elementCount
        self.fileContents = []
        self.results = []
        print(inputFiles, outputFile)

    def fetchFileData(self, fileName:str):  # sourcery skip: raise-specific-error
        directoryPath = os.path.join(os.getcwd(), "Data")
        fileContent = None
        try:
            with open(f"{directoryPath}/{self.root_job_id}/{fileName}.txt", "r") as file:
                fileContent = file.read()
        except FileNotFoundError:
            raise
        fileContent = fileContent.strip("[]")
        elementList = fileContent.split(",")
        elementList = [int(element) for element in elementList]
        self.fileContents.append(elementList)

    def getFiles(self):
        for inputFile in self.inputFiles:
            try:
                self.fetchFileData(inputFile)
            except FileNotFoundError:
                raise 

    def computeSum(self):
        for values in zip(*self.fileContents):
            self.results.append(sum(values))

    def computeAverage(self):
        self.computeSum()
        self.results = [value/(self.elementCount) for value in self.results]

    def computeResults(self):
        try:
            self.getFiles()
        except FileNotFoundError:
            raise
        if(self.isAverage == True):
            self.computeAverage()
        else:
            self.computeSum()
        directoryPath=os.path.relpath("Data",os.getcwd())
        with open(f"{directoryPath}/{self.root_job_id}/{self.outputFile}.txt", "w") as fileContent:
            fileContent.write("[" + (','.join(str(result) for result in self.results)) + "]")
        fileContent.close()