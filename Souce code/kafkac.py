#!/usr/bin/python3
from kafka import KafkaProducer
import tkinter
from kafka import KafkaConsumer
from json import loads
def takeInput():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    age1 = int(age.get())
    sex1=int(sex.get())
    chestpain1=int(chestPain.get())
    trestbps1 = int(rbp.get())
    chol1 = int (serumChol.get())
    fbs1=int(FBS.get())
    ecg1=int(ECG.get())
    thalach1 = int(thalach.get())
    exang1 = int(exang.get())
    oldpeak1 = float(oldpeak.get())
    slope1=int(slope.get())
    ca1=int(ca.get())
    thal1=int(thal.get())
    msg = f'{age1},{sex1},{chestpain1},{trestbps1},{chol1},{fbs1},{ecg1},{thalach1},{exang1},{oldpeak1},{slope1},{ca1},{thal1},{1000}'
    producer.send('weather', bytes(msg, encoding='utf8'))
    print(f'sending data to kafka')
    consumer = KafkaConsumer('testing', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000)
                      
    for message in consumer:
    	message = message.value.decode('utf-8')
    	message = message[14:17]
    
    	substituteWindow = tkinter.Tk()
    	substituteWindow.geometry('640x480-8-200')
    	substituteWindow.title("RESULT PREDICTION")
    
    	substituteWindow.columnconfigure(0, weight=2)
    	substituteWindow.columnconfigure(1, weight=1)
    	substituteWindow.columnconfigure(2, weight=2)
    	substituteWindow.columnconfigure(3, weight=2)
    	substituteWindow.rowconfigure(0, weight=1)
    	substituteWindow.rowconfigure(1, weight=10)
    	substituteWindow.rowconfigure(2, weight=10)
    	substituteWindow.rowconfigure(3, weight=1)
    	substituteWindow.rowconfigure(4, weight=1)
    	substituteWindow.rowconfigure(5, weight=1)
    
    	if message == "1.0":
        	label1 = tkinter.Label(substituteWindow, text="HEART DISEASE DETECTED", font=('Times', -35), fg="red")
        	label1.grid(row=1, column=1, columnspan=6)
        	label2 = tkinter.Label(substituteWindow, text="PLEASE VISIT NEAREST CARDIOLOGIST AT THE EARLIEST", font=('Times', -20), fg='red')
        	label2.grid(row=2, column=1, columnspan=6)
        
    	else: 
        	label1 = tkinter.Label(substituteWindow, text="NO DETECTIOIN OF HEART DISEASES", font=('Times', -35), fg='green' )
        	label1.grid(row=1, column=1, columnspan=6)
        	label2 = tkinter.Label(substituteWindow, text="Do not forget to exercise daily. ", font=('Times', -20), fg='green')
        	label2.grid(row=2, column=1, columnspan=6)      
        

    	substituteWindow.mainloop()
    	
mainWindow = tkinter.Tk()
mainWindow.geometry('640x480-8-200')
mainWindow['padx']=20
mainWindow.title("HEART DISEASE PREDICTION")

mainWindow.columnconfigure(0, weight=2)
mainWindow.columnconfigure(1, weight=1)
mainWindow.columnconfigure(2, weight=2)
mainWindow.columnconfigure(3, weight=2)
mainWindow.rowconfigure(0, weight=0)
mainWindow.rowconfigure(1, weight=0)
mainWindow.rowconfigure(2, weight=1)
mainWindow.rowconfigure(3, weight=1)
mainWindow.rowconfigure(4, weight=1)
mainWindow.rowconfigure(5, weight=1)
mainWindow.rowconfigure(6, weight=1)
mainWindow.rowconfigure(7, weight=1)
mainWindow.rowconfigure(8, weight=10)


label1 = tkinter.Label(mainWindow, text="HEART DISEASE PREDICTION MODEL", font=('Times', -35), bg='#ff8000')
label1.grid(row=0, column=0, columnspan=6)
label2 = tkinter.Label(mainWindow, text="Enter the details carefully", font=('Times', -20) , fg='white', bg='#ff00bf' )
label2.grid(row=1, column=0, columnspan=6)
ageFrame = tkinter.LabelFrame(mainWindow, text="Age(yrs)")
ageFrame.grid(row=2, column=0)
ageFrame.config(font=("Courier", -15))
age= tkinter.Entry(ageFrame)
age.grid(row=2, column=2, sticky='nw')

sexFrame = tkinter.LabelFrame(mainWindow, text="Sex")
sexFrame.grid(row=2, column=1)
sexFrame.config(font=("Courier", -15))
sex= tkinter.Entry(sexFrame)
sex.grid(row=2, column=2, sticky='nw')

chestPainFrame = tkinter.LabelFrame(mainWindow, text="CP (0-4)")
chestPainFrame.grid(row=2, column=2)
chestPainFrame.config(font=("Courier", -15))
chestPain= tkinter.Entry(chestPainFrame)
chestPain.grid(row=2, column=2, sticky='nw')


rbpFrame = tkinter.LabelFrame(mainWindow, text="RBP (94-200)")
rbpFrame.grid(row=3, column=0)
rbpFrame.config(font=("Courier", -15))
rbp= tkinter.Entry(rbpFrame)
rbp.grid(row=2, column=2, sticky='nw')

serumCholFrame = tkinter.LabelFrame(mainWindow, text="Serum Chol")
serumCholFrame.grid(row=3, column=1)
serumCholFrame.config(font=("Courier", -15))
serumChol = tkinter.Entry(serumCholFrame)
serumChol.grid(row=2, column=2, sticky='n')

FBSFrame = tkinter.LabelFrame(mainWindow, text="Fasting BP(0-4)")
FBSFrame.grid(row=3, column=2)
FBSFrame.config(font=("Courier", -15))
FBS= tkinter.Entry(FBSFrame)
FBS.grid(row=2, column=2, sticky='nw')

ECGFrame = tkinter.LabelFrame(mainWindow, text="ECG (0,1,2)")
ECGFrame.grid(row=4, column=0)
ECGFrame.config(font=("Courier", -15))
ECG = tkinter.Entry(ECGFrame)
ECG.grid(row=2, column=2, sticky='nw')


thalachFrame = tkinter.LabelFrame(mainWindow, text="thalach(71-202)")
thalachFrame.grid(row=4, column=1)
thalachFrame.config(font=("Courier", -15))
thalach = tkinter.Entry(thalachFrame)
thalach.grid(row=2, column=2, sticky='nw')

exangFrame = tkinter.LabelFrame(mainWindow, text="exAngina(0/1)")
exangFrame.grid(row=4, column=2)
exangFrame.config(font=("Courier", -15))
exang = tkinter.Entry(exangFrame)
exang.grid(row=2, column=2, sticky='nw')

oldpeakFrame = tkinter.LabelFrame(mainWindow, text="Old Peak(0-6.2)")
oldpeakFrame.grid(row=5, column=0)
oldpeakFrame.config(font=("Courier", -15))
oldpeak = tkinter.Entry(oldpeakFrame)
oldpeak.grid(row=2, column=2, sticky='nw')
  
slopeFrame = tkinter.LabelFrame(mainWindow, text="Slope(0,1,2)")
slopeFrame.grid(row=5, column=1)
slopeFrame.config(font=("Courier", -15))
slope = tkinter.Entry(slopeFrame)
slope.grid(row=2, column=2, sticky='nw')

caFrame = tkinter.LabelFrame(mainWindow, text=" C. A (0-3)")
caFrame.grid(row=5, column=2)
caFrame.config(font=("Courier", -15))
ca = tkinter.Entry(caFrame)
ca.grid(row=2, column=2, sticky='nw')


thalFrame = tkinter.LabelFrame(mainWindow, text=" THAL(0,1,2,3)")
thalFrame.grid(row=6, column=1)
thalFrame.config(font=("Courier", -15))
thal = tkinter.Entry(thalFrame)
thal.grid(row=2, column=2, sticky='nw')


analyseButton = tkinter.Button(mainWindow, text="ANALYZE/ PREDICT", font=('Times', -15), bg = 'red', command=takeInput)
analyseButton.grid(row=8, column=0, columnspan=10)


mainWindow.mainloop()


