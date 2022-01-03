import tkinter as tk
import time
from tkinter import *
from tkinter import messagebox
import psycopg2 as p
import mysql.connector as mysql
import sales_order_fun as fun
from PIL import ImageTk ,Image


class SampleApp(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        self.frame = tk.Frame(self)
        #self.frame.pack(side="top", fill = "both", expand=True)
        
        width= self.winfo_screenwidth() 
        height= self.winfo_screenheight()
        self.geometry("%dx%d" % (width, height))
        self.title("SETTINGS")
        self.configure(background='light green')
        self.protocol("WM_DELETE_WINDOW", self.disable_event)
        
        '''img=ImageTk.PhotoImage(Image.open("logo.png").resize((90, 90)))
        self.lab = tk.Label(image=img,width=48,height = 48)
        self.lab.pack(in_=self.frame)
        self.lab.place(x=80,y=105)'''
        #p1 = PhotoImage(file = 'logo.png')
        #self.iconphoto(False, p1)
        
        self.label = tk.Label(self, width=27, font=("bold", 25),text = "SOFTMUSK PVT LTD")
        self.label.pack(in_=self.frame)
        self.label.place(x=80,y=105)

        
        self.label = tk.Label(self, width=48, font=("bold", 15),text = "Sales Order Status")
        self.label.pack(in_=self.frame)
        self.label.place(x=80,y=165)
        
        self.button0 = tk.Button(self, text='State',width=20,bg='brown',font=("bold", 15),fg='white',command=self.state_call)
        self.button0.place(x=80,y=230)

        self.button1 = tk.Button(self, text='City',width=20,bg='brown',font=("bold", 15),fg='white',command=self.city_call)
        self.button1.place(x=80,y=290)

        self.button2 = tk.Button(self, text='Area',width=20,bg='brown',font=("bold", 15),fg='white',command=self.area_call)
        self.button2.place(x=80,y=350)

        self.button3 = tk.Button(self, text='Customer',width=20,bg='brown',font=("bold", 15),fg='white',command=self.customer_call)
        self.button3.place(x=80,y=410)

        self.button4 = tk.Button(self, text='Manufacturer',width=20,bg='brown',font=("bold", 15),fg='white',command=self.manufacturer_call)
        self.button4.place(x=80,y=470)

        self.button5 = tk.Button(self, text='Category',width=20,bg='brown',font=("bold", 15),fg='white',command=self.category_call)
        self.button5.place(x=380,y=230)

        self.button6 = tk.Button(self, text='Pack',width=20,bg='brown',font=("bold", 15),fg='white',command=self.pack_call)
        self.button6.place(x=380,y=290)

        self.button7 = tk.Button(self, text='Products',width=20,bg='brown',font=("bold", 15),fg='white',command=self.products_call)
        self.button7.place(x=380,y=350)

        self.button8 = tk.Button(self, text='Inventory',width=20,bg='brown',font=("bold", 15),fg='white',command=self.inventory_call)
        self.button8.place(x=380,y=410)

        self.button9 = tk.Button(self, text='OutStanding',width=20,bg='brown',font=("bold", 15),fg='white',command=self.outstanding_call)
        self.button9.place(x=380,y=470)

        self.button10 = tk.Button(self, text='ALL',width=20,bg='brown',font=("bold", 15),fg='white',command=self.allfun)
        self.button10.place(x=220,y=530)

        self.button11 = tk.Button(self, text='Settings',width=20,bg='black',font=("bold", 15),fg='white',command=self.settings_call)
        self.button11.place(x=90,y=590)
        
        self.buttonclose = tk.Button(self, text='Exit',width=20,bg='black',font=("bold", 15),fg='white',command=self.close)
        self.buttonclose.place(x=370,y=590)
        
        self.label1 = tk.Label(self, width=48,bg='light green', font=("bold", 10),text = "Powered by Softmusk info pvt ltd")
        self.label1.pack(in_=self.frame)
        self.label1.place(x=132,y=650)
        
        scrollbar = Scrollbar(self)
        scrollbar.pack( side = RIGHT, fill=Y )
        
        self.mylist = tk.Listbox(self, yscrollcommand = scrollbar.set , width=100,height=29,font=("bold", 10))
        self.mylist.place(x=90,y=17)
        self.mylist.pack( side = RIGHT, fill = BOTH )
        scrollbar.config( command = self.mylist.yview )
        
        # state function call
    def state_call(self): 
        self.disabled_buttons()
        #self.config(cursor="none")
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        fun.state()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()


        # City function call
    def city_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        fun.city()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Area function call
    def area_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.area()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Customer function call
    def customer_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.customer()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Manufacturer function call
    def manufacturer_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.manufacturer()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Category function call
    def category_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.category()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Pack function call
    def pack_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.pack()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Products function call
    def products_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading...")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.product()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Inventory function call
    def inventory_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.inventory()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # OutStanding function call
    def outstanding_call(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.outstanding()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully...")
        self.enable_buttons()

        # Settings function call
    def settings_call(self):
        self.destroy()
        import sales_order

        # all function call
    def allfun(self):
        self.disabled_buttons()
        self.label.config(text = "Uploading... ")
        self.label.update_idletasks()
        #self.disabled_buttons()
        fun.state()
        self.update_log()
        fun.city()
        self.update_log()
        fun.area()
        self.update_log()
        fun.customer()
        self.update_log()
        fun.manufacturer()
        self.update_log()
        fun.category()
        self.update_log()
        fun.pack()
        self.update_log()
        fun.product()
        self.update_log()
        fun.inventory()
        self.update_log()
        fun.outstanding()
        self.update_log()
        self.label.config(text = "Data Uploaded Successfully... ")
        self.enable_buttons()
        
        # Close function
    def close(self):
        self.destroy()

    def update_log(self):
        file1 = open("log_file.txt", "r")
        for line in file1:
            self.mylist.insert(END,  str(line))
        file1.close()

    def disabled_buttons(self):
        self.button0.config(state="disabled")
        self.button1.config(state="disabled")
        self.button2.config(state="disabled")
        self.button3.config(state="disabled")
        self.button4.config(state="disabled")
        self.button5.config(state="disabled")
        self.button6.config(state="disabled")
        self.button7.config(state="disabled")
        self.button8.config(state="disabled")
        self.button9.config(state="disabled")
        self.button10.config(state="disabled")
        self.button11.config(state="disabled")
        self.buttonclose.config(state="disable")
        
    def enable_buttons(self):
        if self.button0["state"] == DISABLED:
            self.button0.config(state="normal")
            self.button1.config(state="normal")
            self.button2.config(state="normal")
            self.button3.config(state="normal")
            self.button4.config(state="normal")
            self.button5.config(state="normal")
            self.button6.config(state="normal")
            self.button7.config(state="normal")
            self.button8.config(state="normal")
            self.button9.config(state="normal")
            self.button10.config(state="normal")
            self.button11.config(state="normal")
            self.buttonclose.config(state="normal")
        else:
            pass
        
    def disable_event(self):
        pass

def main():
    app = SampleApp()
    app.mainloop()  
    return 0
SampleApp()
if __name__ == '__main__':
    main()