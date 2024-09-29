#!/ myenv/bin python3

import json

#Create a program based on Solar Calc spreadsheet 

volts = [6,12,24,32,48]


#open json file and convert to dictionary 
def file_read():
    with open(file, "r", encoding = "utf-8") as r:
        data = json.loads(r.read())
        return data

#write to json file
def file_write(write):
    with open(file, "w", encoding = "utf-8") as w:
         json.dump(write,w)


def json_create():
    equipment = {"equipment":{"#":"$"}}
    file_write(equipment)


def  equipment():
    with file_read() as ui:
        while True:
            user_input = input("What is the name of the equipment? ").strip()

            match = [item for item in data.values() if user_input in str(item)]
            
            if match:  
                print("This equipment is already added")


            file_write(user_input)


json_create()
equipment()
        


