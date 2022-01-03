from tkinter import *
import sqlite3
from tkinter import messagebox

root = Tk()
width= root.winfo_screenwidth() 
height= root.winfo_screenheight()
#setting tkinter window size
root.geometry("%dx%d" % (width, height))
root.title("SETTINGS")
root.configure(background='light green')
#postgres
p_hostname=StringVar()
p_dbname=StringVar()
p_username = StringVar()
p_password= StringVar()
#mysql
s_hostname=StringVar()
s_dbname=StringVar()
s_username = StringVar()
s_password= StringVar()

#reset button

def Reset():
    p_hostname.set("")
    p_dbname.set("")
    p_username.set("")
    p_password.set("")

    s_hostname.set("")
    s_dbname.set("")
    s_username.set("")
    s_password.set("")



#store to db
def database():
    if len(p_hostname.get() and p_dbname.get() and p_username.get() and p_password.get() and s_hostname.get() and s_dbname.get() and s_username.get() and s_password.get())!=0:
      phostname=p_hostname.get()
      pdbname=p_dbname.get()
      pusername=p_username.get()
      p_passw=p_password.get()

      shostname=s_hostname.get()
      sdbname=s_dbname.get()
      susername=s_username.get()
      s_passw=s_password.get()
      

      #conn = sqlite3.connect('Sales_Order.db')

      #with conn:
        # cursor=conn.cursor()
      cursor.execute('''CREATE TABLE IF NOT EXISTS Sales_Order(SALESID INT PRIMARY KEY  , POSTGRES_HOSTNAME VARCHAR NOT NULL, POSTGRES_DBNAME VARCHAR NOT NULL, POSTGRES_USERNAME VARCHAR NOT NULL, POSTGRES_PASSW VARCHAR NOT NULL,MYSQL_HOSTNAME VARCHAR NOT NULL, MYSQL_DBNAME VARCHAR NOT NULL, MYSQL_USERNAME VARCHAR NOT NULL, MYSQL_PASSW VARCHAR NOT NULL)''')
      cursor.execute('INSERT OR REPLACE INTO Sales_Order (SALESID,POSTGRES_HOSTNAME, POSTGRES_DBNAME,POSTGRES_USERNAME,POSTGRES_PASSW,MYSQL_HOSTNAME, MYSQL_DBNAME, MYSQL_USERNAME, MYSQL_PASSW) VALUES (?,?,?,?,?,?,?,?,?) ',(1,phostname,pdbname,pusername,p_passw,shostname,sdbname,susername,s_passw))
      my_conn.commit()
      my_conn.close()
      root.destroy()
      import home2

       

         
    else:
          messagebox.showinfo("ALERT","Values Missing")


             #gui buttons and labels for postgres
label_0 = Label(root, text="SOFTMUSK PVT LTD",width=20,font=("bold", 35))
label_0.place(x=370,y=17)

label_0 = Label(root, text="POSTGRES",width=10,font=("bold", 20))
label_0.place(x=395,y=110)

#hostname
label_1 = Label(root, text="HostName",width=20,font=("bold", 15))
label_1.place(x=80,y=170)
entry_1_p = Entry(root,textvar=p_hostname, width=40,font=("bold", 10))
entry_1_p.place(x=350,y=175)

#dbname
label_2 = Label(root, text="DB Name",width=20,font=("bold", 15))
label_2.place(x=80,y=240)
entry_2_p = Entry(root,textvar=p_dbname, width=40,font=("bold", 10))
entry_2_p.place(x=350,y=245)
#username
label_2 = Label(root, text="User",width=20,font=("bold", 15))
label_2.place(x=80,y=310)
entry_3_p = Entry(root,textvar=p_username, width=40,font=("bold", 10))
entry_3_p.place(x=350,y=315)

#password
label_2 = Label(root, text="Password",width=20,font=("bold", 15))
label_2.place(x=80,y=380)
entry_4_p = Entry(root,textvar=p_password, width=40,font=("bold", 10))
entry_4_p.place(x=350,y=385)

            #gui buttons and labels for mysql
label_0 = Label(root, text="MYSQL",width=10,font=("bold", 20))
label_0.place(x=790,y=108)

#hostname
entry_1_m = Entry(root,textvar=s_hostname, width=50,font=("bold", 10))
entry_1_m.place(x=740,y=175)

#dbname
entry_2_m = Entry(root,textvar=s_dbname, width=50,font=("bold", 10))
entry_2_m.place(x=740,y=245)
#username
entry_3_m = Entry(root,textvar=s_username, width=50,font=("bold", 10))
entry_3_m.place(x=740,y=315)

#password
entry_4_m = Entry(root,textvar=s_password, width=50,font=("bold", 10))
entry_4_m.place(x=740,y=385)

Button(root, text='Submit',width=20,bg='brown',fg='white',command=database).place(x=400,y=470)

#Button(root, text='Update',width=20,bg='dark blue',fg='white',command=database).place(x=270,y=350)

Button(root, text='Clear',width=20,bg='Red',fg='white',command=Reset).place(x=798,y=467)

label1 = Label(root, width=48, font=("bold", 10),bg='light green',text = "Powered by Softmusk info pvt ltd")
label1.place(x=490,y=580)

my_conn = sqlite3.connect('Sales_Order.db')
with my_conn:
    cursor=my_conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Sales_Order(SALESID INT PRIMARY KEY  , POSTGRES_HOSTNAME VARCHAR NOT NULL, POSTGRES_DBNAME VARCHAR NOT NULL, POSTGRES_USERNAME VARCHAR NOT NULL, POSTGRES_PASSW VARCHAR NOT NULL,MYSQL_HOSTNAME VARCHAR NOT NULL, MYSQL_DBNAME VARCHAR NOT NULL, MYSQL_USERNAME VARCHAR NOT NULL, MYSQL_PASSW VARCHAR NOT NULL)''')
    r_set=my_conn.execute('''SELECT * from Sales_Order ''')
    r_set_count = my_conn.execute('''SELECT count(*) FROM Sales_Order''')
    for column in r_set:
        entry_1_p.insert(END, column[1])
        entry_2_p.insert(END, column[2])
        entry_3_p.insert(END, column[3])
        entry_4_p.insert(END, column[4])
        entry_1_m.insert(END, column[5])
        entry_2_m.insert(END, column[6])
        entry_3_m.insert(END, column[7])
        entry_4_m.insert(END, column[8])
        my_conn.commit()
    
        

root.mainloop()

