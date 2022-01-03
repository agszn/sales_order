import os
import psycopg2 as p
import sqlite3
import psycopg2.extras
import mysql.connector as mysql
import datetime

now = datetime.datetime.now()
conn = sqlite3.connect('Sales_Order.db')
with conn:
    cursor = conn.cursor()
data = cursor.execute('select * from Sales_Order where SALESID=1')
conn.commit()

for i in data:
    pg_hostname = i[1]
    pg_dbname = i[2]
    pg_username = i[3]
    pg_pass = i[4]
    mysql_hostname = i[5]
    mysql_dbname = i[6]
    mysql_username = i[7]
    mysql_pass = i[8]
res = []

try:
    #pg_conn = p.connect(host="122.166.145.60", database="chinnu_2122", user="nodes", password="eval")
    #pg_cursor = pg_conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    #mysql_conn = mysql.connect(host="chinnu-2021.cz9jpqqrrzui.ap-south-1.rds.amazonaws.com", database="softmusk", user="root", password="1qaz2wsx")
    #mysql_cursor = mysql_conn.cursor()
    pg_conn = p.connect(host=pg_hostname, database=pg_dbname, user=pg_username, password=pg_pass)
    pg_cursor = pg_conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    mysql_conn = mysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_username, password=mysql_pass)
    mysql_cursor = mysql_conn.cursor()
    
    #state table
    def state():
        print('\n'+'========== State ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== State ==========')
        f.close()
        state = "SELECT * from state"
        pg_cursor.execute(state)
        state_res = pg_cursor.fetchall()
        for row in state_res:
            statecode = str(row['st_code'])
            if(row['st_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO states (state_id, state_name) values('"+statecode+"', '"+row['st_nm']+"') ON DUPLICATE KEY UPDATE state_name = '"+row['st_nm']+"'")
                    mysql_conn.commit()
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    print(row)
                    print("==============================")
        print("data inserted to state table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to state table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    def city():
        #city table
        print('\n'+'========== City ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== City ==========')
        f.close()
        city = "SELECT ct_code, ct_nm, ct_state from city"
        pg_cursor.execute(city)
        city_res = pg_cursor.fetchall()
        for row in city_res:
            citycode = str(row['ct_code'])
            statecode = str(row['ct_state'])
            if(row['ct_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO cities (city_id, state_id, city_name) values('"+citycode+"', '"+statecode+"', '"+row['ct_nm']+"') ON DUPLICATE KEY UPDATE state_id = '"+statecode+"', city_name = '"+row['ct_nm']+"'")
                    mysql_conn.commit()
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    print(row)
                    print("==============================")
        print("data inserted to city table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to city table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()
    
    def area():
        #Area table
        print('========== Area ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Area ==========')
        f.close()
        area = "SELECT * from area"
        pg_cursor.execute(area)
        area_res = pg_cursor.fetchall()
        for row in area_res:
            areacode = str(row['ar_code'])
            if(row['ar_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO areas (area_id, area_name) values('"+areacode+"', '"+row['ar_nm']+"') ON DUPLICATE KEY UPDATE area_name = '"+row['ar_nm']+"'")
                    mysql_conn.commit()
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    
                    print(row)
                    print("==============================")
        print("data inserted to area table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to area table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    def customer():
    #Customer/ledger table
        print('========== Customer ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Customer ==========')
        f.close()
        ledger = "SELECT * from ledger_acts where type=26"
        count_ledger = "SELECT COUNT(*) FROM TableName"
        pg_cursor.execute(ledger)
        ledger_res = pg_cursor.fetchall()            
        for row in ledger_res :
            code = str(row['code'])
            mobile = str(row['ph1'])
            state = str(row['state'])
            city = str(row['city'])
            area = str(row['area'])
            pin = str(row['pin'])
            op_bal = str(row['op_bal'])
            clos_bal = str(row['clos_bal'])
            try:
                mysql_cursor.execute("insert into customers (customer_id, customer_name, mobile_no, email, address_1, address_2, state_id, city_id, pincode, area_id, op_bal, clos_bal) values('"+code+"', '"+row['name']+"', '"+mobile+"', '"+row['email']+"', '"+row['adr1']+"', '"+row['adr2']+"', '"+state+"', '"+city+"', '"+pin+"', '"+area+"', '"+op_bal+"', '"+clos_bal+"') ON DUPLICATE KEY UPDATE customer_name = '"+row['name']+"', mobile_no = '"+mobile+"', email = '"+row['email']+"', address_1 = '"+row['adr1']+"', address_2 = '"+row['adr2']+"', state_id = '"+state+"', city_id = '"+city+"', pincode = '"+pin+"', area_id = '"+area+"', op_bal = '"+op_bal+"', clos_bal = '"+clos_bal+"'")
                mysql_conn.commit() 
                res.append({ "code":code, "name":row['name'], "mobile_no1":row['ph1'], "mobile_no2":row['ph2'], 
                "email":row['email'], "address_1":row['adr1'], "address_2":row['adr2'], "city":row['city'], "state":row['state'],
                "pincode":row['pin'] }) 
            except mysql.Error as err:
                res.append("Something went wrong: {}".format(err))
                f = open("log.txt", "a")
                f.write('\n'+str(err) +'\n'+str(row))
                f.close()
                f = open("log_file.txt", "a")
                f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                f.close()
                print("==============================")
                print(err)
                print(row)
                print("==============================")

        print("data inserted to customer table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to customer table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    #Manufacturer table
    def manufacturer():
        print('========== Manufacturer ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Manufacturer ==========')
        f.close()
        manufacturer = "SELECT * from manf"
        pg_cursor.execute(manufacturer)
        manufacturer_res = pg_cursor.fetchall()
        for row in manufacturer_res:
            mancode = str(row['man_code'])
            mobile = str(row['man_ph1'])
            state = str(row['man_state'])
            city = str(row['man_city'])
            if(row['man_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO manufacturers (manufacturer_id, name, short_name, mobile_no, email, gstin, drug_licence_1, drug_licence_2, address_1, address_2, state_id, city_id) values('"+mancode+"', '"+row['man_nm']+"', '"+row['man_snm']+"', '"+mobile+"', '"+row['man_email']+"', '"+row['man_tin1']+"', '"+row['man_dl1']+"', '"+row['man_dl2']+"', '"+row['man_adr1']+"', '"+row['man_adr2']+"', '"+state+"', '"+city+"') ON DUPLICATE KEY UPDATE name = '"+row['man_nm']+"', short_name = '"+row['man_snm']+"', mobile_no = '"+mobile+"', email = '"+row['man_email']+"', gstin = '"+row['man_tin1']+"', drug_licence_1 = '"+row['man_dl1']+"', drug_licence_2 = '"+row['man_dl2']+"', address_1 = '"+row['man_adr1']+"', address_2 = '"+row['man_adr2']+"', state_id = '"+state+"', city_id = '"+city+"'")
                    mysql_conn.commit()
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    print(row)
                    print("==============================")
        print("data inserted to manufacturer table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to manufacturer table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()
    
    #Categories table
    def category():
        print('========== Category ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Category ==========')
        f.close()
        category = "SELECT * from cat"
        pg_cursor.execute(category)
        category_res = pg_cursor.fetchall()

        for row in category_res:
            catcode = str(row['cat_code'])
            if(row['cat_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO categories (category_id, category_name) values('"+catcode+"', '"+row['cat_nm']+"') ON DUPLICATE KEY UPDATE category_name = '"+row['cat_nm']+"'")
                    mysql_conn.commit()
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    print(row)
                    print("==============================")
        print("data inserted to category table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to category table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    #Packs table
    def pack():
        print('========== Pack ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Pack ==========')
        f.close()
        pack = "SELECT * from pack"
        pg_cursor.execute(pack)
        pack_res = pg_cursor.fetchall()

        for row in pack_res:
            packcode = str(row['pk_code'])
            if(row['pk_nm'] != 'UNDEFINED'):
                try:
                    mysql_cursor.execute("INSERT INTO packs (pack_id, pack_name) values('"+packcode+"', '"+row['pk_nm']+"') ON DUPLICATE KEY UPDATE pack_name = '"+row['pk_nm']+"'")
                    mysql_conn.commit() 
                except mysql.Error as err:
                    res.append("Something went wrong: {}".format(err))
                    f = open("log.txt", "a")
                    f.write('\n'+str(err) +'\n'+str(row))
                    f.close()
                    f = open("log_file.txt", "a")
                    f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                    f.close()
                    print("==============================")
                    print(err)
                    print(row)
                    print("==============================")
        print("data inserted to pcks table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to pack table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    #Product table
    def product():
        print('========== Products ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== State ==========')
        f.close()
        product = "SELECT * from prod"
        pg_cursor.execute(product)
        product_res = pg_cursor.fetchall()

        for row in product_res:
            prodcode = str(row['prod_code'])
            prod_nm = str(row['prod_nm'])
            prod_manf = str(row['prod_manf'])
            prod_cat = str(row['prod_cat'])
            prod_rack = str(row['prod_rack'])
            prod_cmn = str(row['prod_cmn'])
            prod_min = str(row['prod_min'])
            prod_max = str(row['prod_max'])
            prod_disc_cab = str(row['prod_disc_cab'])
            prod_disc_crb = str(row['prod_disc_crb'])
            prod_box_pk = str(row['prod_box_pk'])
            prod_blis_pk = str(row['prod_blis_pk'])
            prod_pack = str(row['prod_pack'])
            link = str(row['link'])
            prod_discontinued = str(row['prod_discontinued'])
            prod_creation_date = str(row['prod_creation_date'])
            prod_com_code = str(row['prod_com_code'])
            prod_exp_applicable = str(row['prod_exp_applicable'])
            prod_batch_applicable = str(row['prod_batch_applicable'])
            prod_tax_type = str(row['prod_tax_type'])
            prod_as_per_input = str(row['prod_as_per_input'])
            prod_qty_per_pack = str(row['prod_qty_per_pack'])
            igst = str(row['igst'])
            cgst = str(row['cgst'])
            sgst = str(row['sgst'])
            cess = str(row['cess'])
            gst_slab = str(row['gst_slab'])
            hsn_code = str(row['hsn_code'])
            prod_sup_prod_code = str(row['prod_sup_prod_code'])
            prod_drug_type = str(row['prod_drug_type'])
            prod_hsn_code = str(row['prod_hsn_code'])
            prod_disc_retail = str(row['prod_disc_retail'])
            sub_hsn_code = str(row['sub_hsn_code'])
            prod_ignore_mrp = str(row['prod_ignore_mrp'])
            prod_non_returnable = str(row['prod_non_returnable'])
            try:
                mysql_cursor.execute("INSERT INTO products (product_id, prod_name, manufacturer_id, category_id, prod_rack, prod_cmn, prod_min, prod_max, prod_disc_cab, prod_disc_crb, prod_box_pk, prod_blis_pk, pack_id, link, prod_discontinued, prod_creation_date, prod_com_code, prod_exp_applicable, prod_batch_applicable, prod_tax_type, prod_as_per_input, prod_qty_per_pack, igst, cgst, sgst, cess, gst_slab, hsn_code, prod_sup_prod_code, prod_drug_type, prod_hsn_code, prod_disc_retail, sub_hsn_code, prod_ignore_mrp, prod_non_returnable) values('"+prodcode+"', '"+prod_nm+"', '"+prod_manf+"', '"+prod_cat+"', '"+prod_rack+"', '"+prod_cmn+"', '"+prod_min+"', '"+prod_max+"', '"+prod_disc_cab+"', '"+prod_disc_crb+"', '"+prod_box_pk+"', '"+prod_blis_pk+"', '"+prod_pack+"', '"+link+"', '"+prod_discontinued+"', '"+prod_creation_date+"', '"+prod_com_code+"', '"+prod_exp_applicable+"', '"+prod_batch_applicable+"', '"+prod_tax_type+"', '"+prod_as_per_input+"', '"+prod_qty_per_pack+"', '"+igst+"', '"+cgst+"', '"+sgst+"', '"+cess+"', '"+gst_slab+"', '"+hsn_code+"', '"+prod_sup_prod_code+"', '"+prod_drug_type+"', '"+prod_hsn_code+"', '"+prod_disc_retail+"', '"+sub_hsn_code+"', '"+prod_ignore_mrp+"', '"+prod_non_returnable+"') ON DUPLICATE KEY UPDATE prod_name = '"+prod_nm+"', manufacturer_id = '"+prod_manf+"', category_id = '"+prod_cat+"', prod_rack = '"+prod_rack+"', prod_cmn = '"+prod_cmn+"', prod_min = '"+prod_min+"', prod_max = '"+prod_max+"', prod_disc_cab = '"+prod_disc_cab+"', prod_disc_crb = '"+prod_disc_crb+"', prod_box_pk = '"+prod_box_pk+"', prod_blis_pk = '"+prod_blis_pk+"', pack_id = '"+prod_pack+"', link = '"+link+"', prod_discontinued = '"+prod_discontinued+"', prod_creation_date = '"+prod_creation_date+"', prod_com_code = '"+prod_com_code+"', prod_exp_applicable = '"+prod_exp_applicable+"', prod_batch_applicable = '"+prod_batch_applicable+"', prod_tax_type = '"+prod_tax_type+"', prod_as_per_input = '"+prod_as_per_input+"', prod_qty_per_pack = '"+prod_qty_per_pack+"', igst = '"+igst+"', cgst = '"+cgst+"', sgst = '"+sgst+"', cess = '"+cess+"', gst_slab = '"+gst_slab+"', hsn_code = '"+hsn_code+"', prod_sup_prod_code = '"+prod_sup_prod_code+"', prod_drug_type = '"+prod_drug_type+"', prod_hsn_code = '"+prod_hsn_code+"', prod_disc_retail = '"+prod_disc_retail+"', sub_hsn_code = '"+sub_hsn_code+"', prod_ignore_mrp = '"+prod_ignore_mrp+"', prod_non_returnable = '"+prod_non_returnable+"'")
                mysql_conn.commit() 
            except mysql.Error as err:
                res.append("Something went wrong: {}".format(err))  
                f = open("log.txt", "a")
                f.write('\n'+str(err) +'\n'+str(row))
                f.close()
                f = open("log_file.txt", "a")
                f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                f.close()
                print("==============================")
                print(err)
                print(row)
                print("==============================")
        print("data inserted to product table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to product table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    #Inventory table
    def inventory():
        print('========== Inventory ==========')
        f = open("log_file.txt", "w")
        f.write('========== Inventory ==========')
        f.close()
        inventory = "SELECT * from inventory"
        pg_cursor.execute(inventory)
        inventory_res = pg_cursor.fetchall()
        
        for row in inventory_res:
            print(str(row['inv_prod_code']))
            inventory_id = str(row['inv_code'])		
            inv_vno = str(row['inv_vno']) 				
            product_id = str(row['inv_prod_code']) 				
            inv_supcode = str(row['inv_supcode']) 			
            inv_vdate = str(row['inv_vdate']) 				
            inv_cr_note_no = str(row['inv_cr_note_no']) 			
            inv_pur_bill_no = str(row['inv_pur_bill_no']) 		
            inv_pur_bill_dt = str(row['inv_pur_bill_dt']) 		
            inv_p_sqty = str(row['inv_p_sqty']) 				
            inv_p_fqty = str(row['inv_p_fqty']) 				
            inv_exp_dt = str(row['inv_exp_dt']) 				
            inv_batch = str(row['inv_batch']) 				
            inv_a_sqty = str(row['inv_a_sqty']) 				
            inv_p_rate = str(row['inv_p_rate']) 				
            inv_s_rate1 = str(row['inv_s_rate1']) 			
            inv_s_rate2 = str(row['inv_s_rate2']) 			
            inv_s_rate3 = str(row['inv_s_rate3']) 			
            inv_mrp = str(row['inv_mrp']) 				
            inv_mrp_in = str(row['inv_mrp_in']) 				
            inv_incl = str(row['inv_incl']) 				
            inv_scheme = str(row['inv_scheme']) 				
            inv_op = str(row['inv_op']) 					
            inv_modify = str(row['inv_modify']) 				
            inv_time_stamp = str(row['inv_time_stamp']) 			
            inv_p_tax = str(row['inv_p_tax']) 				
            inv_p_excise = str(row['inv_p_excise']) 			
            inv_disc = str(row['inv_disc']) 				
            inv_schm_disc = str(row['inv_schm_disc']) 			
            inv_tax_rate = str(row['inv_tax_rate']) 			
            inv_tag = str(row['inv_tag']) 				
            inv_s_rep = str(row['inv_s_rep']) 				
            inv_m_rep = str(row['inv_m_rep']) 				
            inv_tax_type = str(row['inv_tax_type']) 			
            inv_checked = str(row['inv_checked']) 			
            inv_stk_status = str(row['inv_stk_status']) 			
            inv_link = str(row['inv_link']) 				
            inv_rak_id = str(row['inv_rak_id']) 				
            inv_out_state = str(row['inv_out_state']) 			
            inv_m_lock = str(row['inv_m_lock']) 				
            inv_frm_str = str(row['inv_frm_str']) 			
            inv_to_str = str(row['inv_to_str']) 				
            inv_pur_sch_qty = str(row['inv_pur_sch_qty']) 		
            inv_pur_sch_fqty = str(row['inv_pur_sch_fqty']) 		
            inv_sal_sch_qty = str(row['inv_sal_sch_qty']) 		
            inv_sal_sch_fqty = str(row['inv_sal_sch_fqty']) 		
            inv_lr_no = str(row['inv_lr_no']) 				
            inv_lr_dt = str(row['inv_lr_dt']) 				
            inv_lcost = str(row['inv_lcost']) 				
            inv_trn_code = str(row['inv_trn_code']) 			
            inv_trn_no = str(row['inv_trn_no']) 				
            inv_lcost_no_tax = str(row['inv_lcost_no_tax']) 		
            inv_locked = str(row['inv_locked']) 				
            inv_checked_user = str(row['inv_checked_user']) 		
            inv_po_code = str(row['inv_po_code']) 			
            inv_po_no = str(row['inv_po_no']) 				
            inv_reduction_on_mrp = str(row['inv_reduction_on_mrp']) 	
            inv_sal_sch_validity = str(row['inv_sal_sch_validity']) 	
            inv_with_c_form = str(row['inv_with_c_form']) 		
            inv_closing_stk = str(row['inv_closing_stk']) 		
            inv_ref_inv_no = str(row['inv_ref_inv_no']) 			
            inv_ref_inv_dt = str(row['inv_ref_inv_dt']) 			
            inv_cn_non_taxable = str(row['inv_cn_non_taxable']) 		
            inv_locked_user = str(row['inv_locked_user']) 		
            inv_grn_no = str(row['inv_grn_no']) 				
            inv_no_of_cases = str(row['inv_no_of_cases']) 		
            inv_parent_code = str(row['inv_parent_code']) 		
            inv_unact = str(row['inv_unact']) 				
            inv_web_document = str(row['inv_web_document']) 		
            inv_about_to_modify = str(row['inv_about_to_modify']) 	
            inv_new_record = str(row['inv_new_record']) 			
            inv_max_fqty_allotted = str(row['inv_max_fqty_allotted']) 	
            inv_fqty_allotted = str(row['inv_fqty_allotted']) 		
            inv_ref_inv_is_cash = str(row['inv_ref_inv_is_cash']) 	
            inv_barcode_type = str(row['inv_barcode_type']) 		
            inv_barcode = str(row['inv_barcode']) 			
            gst_bill = str(row['gst_bill']) 				
            igst_tax = str(row['igst_tax']) 				
            cgst_tax = str(row['cgst_tax']) 				
            sgst_tax = str(row['sgst_tax']) 				
            cess_tax = str(row['cess_tax']) 				
            pur_type = str(row['pur_type']) 				
            inv_qty_per_pack = str(row['inv_qty_per_pack']) 		
            inv_retail_rate = str(row['inv_retail_rate']) 		
            inv_retail_qty_adjusted = str(row['inv_retail_qty_adjusted']) 
            inv_retail_aqty = str(row['inv_retail_aqty']) 		
            inv_retail_p_sqty = str(row['inv_retail_p_sqty']) 		
            inv_retail_p_fqty = str(row['inv_retail_p_fqty']) 		
            inv_retail_sold_sqty = str(row['inv_retail_sold_sqty']) 	
            inv_retail_sold_fqty = str(row['inv_retail_sold_fqty']) 	
            inv_wholesale_sold_sqty = str(row['inv_wholesale_sold_sqty']) 
            inv_wholesale_sold_fqty = str(row['inv_wholesale_sold_fqty']) 
            inv_s_gst_rate1 = str(row['inv_s_gst_rate1']) 		
            inv_s_gst_rate2 = str(row['inv_s_gst_rate2']) 		
            inv_s_gst_rate3 = str(row['inv_s_gst_rate3']) 		
            inv_retail_gst_rate = str(row['inv_retail_gst_rate']) 	
            inv_ref_inv_vno = str(row['inv_ref_inv_vno']) 		
            inv_cess_rate = str(row['inv_cess_rate']) 			
            inv_reg_type = str(row['inv_reg_type']) 			
            inv_sale_sch_disc = str(row['inv_sale_sch_disc']) 		
            inv_min_qty_for_disc = str(row['inv_min_qty_for_disc']) 	

            try:
                
                mysql_cursor.execute("INSERT INTO inventories (inventory_id, inv_vno, product_id, inv_supcode, inv_vdate, inv_cr_note_no, inv_pur_bill_no, inv_pur_bill_dt, inv_p_sqty, inv_p_fqty, inv_exp_dt, inv_batch, inv_a_sqty, inv_p_rate, inv_s_rate1, inv_s_rate2, inv_s_rate3, inv_mrp, inv_mrp_in, inv_incl, inv_scheme, inv_op, inv_modify, inv_time_stamp, inv_p_tax, inv_p_excise, inv_disc, inv_schm_disc, inv_tax_rate, inv_tag, inv_s_rep, inv_m_rep, inv_tax_type, inv_checked, inv_stk_status, inv_link, inv_rak_id, inv_out_state, inv_m_lock, inv_frm_str, inv_to_str, inv_pur_sch_qty, inv_pur_sch_fqty, inv_sal_sch_qty, inv_sal_sch_fqty, inv_lr_no, inv_lr_dt, inv_lcost, inv_trn_code, inv_trn_no, inv_lcost_no_tax, inv_locked, inv_checked_user, inv_po_code, inv_po_no, inv_reduction_on_mrp, inv_sal_sch_validity, inv_with_c_form, inv_closing_stk, inv_ref_inv_no, inv_ref_inv_dt, inv_cn_non_taxable, inv_locked_user, inv_grn_no, inv_no_of_cases, inv_parent_code, inv_unact, inv_web_document, inv_about_to_modify, inv_new_record, inv_max_fqty_allotted, inv_fqty_allotted, inv_ref_inv_is_cash, inv_barcode_type, inv_barcode, gst_bill, igst_tax, cgst_tax, sgst_tax, cess_tax, pur_type, inv_qty_per_pack, inv_retail_rate, inv_retail_qty_adjusted, inv_retail_aqty, inv_retail_p_sqty, inv_retail_p_fqty, inv_retail_sold_sqty, inv_retail_sold_fqty, inv_wholesale_sold_sqty, inv_wholesale_sold_fqty, inv_s_gst_rate1, inv_s_gst_rate2, inv_s_gst_rate3, inv_retail_gst_rate, inv_ref_inv_vno, inv_cess_rate, inv_reg_type, inv_sale_sch_disc, inv_min_qty_for_disc) values('"+inventory_id+"', '"+inv_vno+"', '"+product_id+"', '"+inv_supcode+"', '"+inv_vdate+"', '"+inv_cr_note_no+"', '"+inv_pur_bill_no+"', '"+inv_pur_bill_dt+"', '"+inv_p_sqty+"', '"+inv_p_fqty+"', '"+inv_exp_dt+"', '"+inv_batch+"','"+inv_a_sqty+"', '"+inv_p_rate+"', '"+inv_s_rate1+"', '"+inv_s_rate2+"', '"+inv_s_rate3+"', '"+inv_mrp+"', '"+inv_mrp_in+"', '"+inv_incl+"', '"+inv_scheme+"', '"+inv_op+"', '"+inv_modify+"', '"+inv_time_stamp+"', '"+inv_p_tax+"', '"+inv_p_excise+"', '"+inv_disc+"', '"+inv_schm_disc+"', '"+inv_tax_rate+"', '"+inv_tag+"', '"+inv_s_rep+"', '"+inv_m_rep+"', '"+inv_tax_type+"', '"+inv_checked+"', '"+inv_stk_status+"', '"+inv_link+"', '"+inv_rak_id+"', '"+inv_out_state+"', '"+inv_m_lock+"', '"+inv_frm_str+"', '"+inv_to_str+"', '"+inv_pur_sch_qty+"', '"+inv_pur_sch_fqty+"', '"+inv_sal_sch_qty+"', '"+inv_sal_sch_fqty+"', '"+inv_lr_no+"', '"+inv_lr_dt+"', '"+inv_lcost+"', '"+inv_trn_code+"', '"+inv_trn_no+"', '"+inv_lcost_no_tax+"', '"+inv_locked+"', '"+inv_checked_user+"', '"+inv_po_code+"', '"+inv_po_no+"', '"+inv_reduction_on_mrp+"', '"+inv_sal_sch_validity+"', '"+inv_with_c_form+"', '"+inv_closing_stk+"', '"+inv_ref_inv_no+"', '"+inv_ref_inv_dt+"', '"+inv_cn_non_taxable+"', '"+inv_locked_user+"', '"+inv_grn_no+"', '"+inv_no_of_cases+"', '"+inv_parent_code+"', '"+inv_unact+"', '"+inv_web_document+"', '"+inv_about_to_modify+"', '"+inv_new_record+"', '"+inv_max_fqty_allotted+"', '"+inv_fqty_allotted+"', '"+inv_ref_inv_is_cash+"', '"+inv_barcode_type+"', '"+inv_barcode+"', '"+gst_bill+"', '"+igst_tax+"', '"+cgst_tax+"', '"+sgst_tax+"', '"+cess_tax+"', '"+pur_type+"', '"+inv_qty_per_pack+"', '"+inv_retail_rate+"', '"+inv_retail_qty_adjusted+"', '"+inv_retail_aqty+"', '"+inv_retail_p_sqty+"', '"+inv_retail_p_fqty+"', '"+inv_retail_sold_sqty+"', '"+inv_retail_sold_fqty+"', '"+inv_wholesale_sold_sqty+"', '"+inv_wholesale_sold_fqty+"', '"+inv_s_gst_rate1+"', '"+inv_s_gst_rate2+"', '"+inv_s_gst_rate3+"', '"+inv_retail_gst_rate+"', '"+inv_ref_inv_vno+"', '"+inv_cess_rate+"', '"+inv_reg_type+"', '"+inv_sale_sch_disc+"', '"+inv_min_qty_for_disc+"') ON DUPLICATE KEY UPDATE inv_vno='"+inv_vno+"', product_id='"+product_id+"', inv_supcode='"+inv_supcode+"', inv_vdate='"+inv_vdate+"', inv_cr_note_no='"+inv_cr_note_no+"', inv_pur_bill_no='"+inv_pur_bill_no+"', inv_pur_bill_dt='"+inv_pur_bill_dt+"', inv_p_sqty='"+inv_p_sqty+"', inv_p_fqty='"+inv_p_fqty+"', inv_exp_dt='"+inv_exp_dt+"', inv_batch='"+inv_batch+"', inv_a_sqty='"+inv_a_sqty+"', inv_p_rate='"+inv_p_rate+"', inv_s_rate1='"+inv_s_rate1+"', inv_s_rate2='"+inv_s_rate2+"', inv_s_rate3='"+inv_s_rate3+"', inv_mrp='"+inv_mrp+"', inv_mrp_in='"+inv_mrp_in+"', inv_incl='"+inv_incl+"', inv_scheme='"+inv_scheme+"', inv_op='"+inv_op+"', inv_modify='"+inv_modify+"', inv_time_stamp='"+inv_time_stamp+"', inv_p_tax='"+inv_p_tax+"', inv_p_excise='"+inv_p_excise+"', inv_disc='"+inv_disc+"', inv_schm_disc='"+inv_schm_disc+"', inv_tax_rate='"+inv_tax_rate+"', inv_tag='"+inv_tag+"', inv_s_rep='"+inv_s_rep+"', inv_m_rep='"+inv_m_rep+"', inv_tax_type='"+inv_tax_type+"', inv_checked='"+inv_checked+"', inv_stk_status='"+inv_stk_status+"', inv_link='"+inv_link+"', inv_rak_id='"+inv_rak_id+"', inv_out_state='"+inv_out_state+"', inv_m_lock='"+inv_m_lock+"', inv_frm_str='"+inv_frm_str+"', inv_to_str='"+inv_to_str+"', inv_pur_sch_qty='"+inv_pur_sch_qty+"', inv_pur_sch_fqty='"+inv_pur_sch_fqty+"', inv_sal_sch_qty='"+inv_sal_sch_qty+"', inv_sal_sch_fqty='"+inv_sal_sch_fqty+"', inv_lr_no='"+inv_lr_no+"', inv_lr_dt='"+inv_lr_dt+"', inv_lcost='"+inv_lcost+"', inv_trn_code='"+inv_trn_code+"', inv_trn_no='"+inv_trn_no+"', inv_lcost_no_tax='"+inv_lcost_no_tax+"', inv_locked='"+inv_locked+"', inv_checked_user='"+inv_checked_user+"', inv_po_code='"+inv_po_code+"', inv_po_no='"+inv_po_no+"', inv_reduction_on_mrp='"+inv_reduction_on_mrp+"', inv_sal_sch_validity='"+inv_sal_sch_validity+"', inv_with_c_form='"+inv_with_c_form+"', inv_closing_stk='"+inv_closing_stk+"', inv_ref_inv_no='"+inv_ref_inv_no+"', inv_ref_inv_dt='"+inv_ref_inv_dt+"', inv_cn_non_taxable='"+inv_cn_non_taxable+"', inv_locked_user='"+inv_locked_user+"', inv_grn_no='"+inv_grn_no+"', inv_no_of_cases='"+inv_no_of_cases+"', inv_parent_code='"+inv_parent_code+"', inv_unact='"+inv_unact+"', inv_web_document='"+inv_web_document+"', inv_about_to_modify='"+inv_about_to_modify+"', inv_new_record='"+inv_new_record+"', inv_max_fqty_allotted='"+inv_max_fqty_allotted+"', inv_fqty_allotted='"+inv_fqty_allotted+"', inv_ref_inv_is_cash='"+inv_ref_inv_is_cash+"', inv_barcode_type='"+inv_barcode_type+"', inv_barcode='"+inv_barcode+"', gst_bill='"+gst_bill+"', igst_tax='"+igst_tax+"', cgst_tax='"+cgst_tax+"', sgst_tax='"+sgst_tax+"', cess_tax='"+cess_tax+"', pur_type='"+pur_type+"', inv_qty_per_pack='"+inv_qty_per_pack+"', inv_retail_rate='"+inv_retail_rate+"', inv_retail_qty_adjusted='"+inv_retail_qty_adjusted+"', inv_retail_aqty='"+inv_retail_aqty+"', inv_retail_p_sqty='"+inv_retail_p_sqty+"', inv_retail_p_fqty='"+inv_retail_p_fqty+"', inv_retail_sold_sqty='"+inv_retail_sold_sqty+"', inv_retail_sold_fqty='"+inv_retail_sold_fqty+"', inv_wholesale_sold_sqty='"+inv_wholesale_sold_sqty+"', inv_wholesale_sold_fqty='"+inv_wholesale_sold_fqty+"', inv_s_gst_rate1='"+inv_s_gst_rate1+"', inv_s_gst_rate2='"+inv_s_gst_rate2+"', inv_s_gst_rate3='"+inv_s_gst_rate3+"', inv_retail_gst_rate='"+inv_retail_gst_rate+"', inv_ref_inv_vno='"+inv_ref_inv_vno+"', inv_cess_rate='"+inv_cess_rate+"', inv_reg_type='"+inv_reg_type+"', inv_sale_sch_disc='"+inv_sale_sch_disc+"', inv_min_qty_for_disc='"+inv_min_qty_for_disc+"'")
                mysql_conn.commit() 
            except mysql.Error as err:
                res.append("Something went wrong: {}".format(err)) 
                f = open("log.txt", "a")
                f.write('\n'+str(err) +'\n'+str(row))
                f.close()
                f = open("log_file.txt", "a")
                f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                f.close() 
                print("==============================")
                print(err)
                print(row)
                print("==============================")
        print("data inserted to inventory table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to inventory table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

    #outstanding table
    def outstanding():
        print('========== Out Standing ==========')
        f = open("log_file.txt", "w")
        f.write('\n'+'========== Out Standing ==========')
        f.close()
        out_standing = "SELECT * from outstandings where v_type='SR'"
        pg_cursor.execute(out_standing)
        out_standing_res = pg_cursor.fetchall()
        
        for row in out_standing_res:
            out_standing_id = str(row['vno'])
            bill_no = str(row['bill_no'])
            bill_dt = str(row['bill_dt'])
            customer_id = str(row['party_act'])
            v_type = str(row['v_type'])
            code = str(row['code'])
            due_dt = str(row['due_dt'])
            ori_dr_amount = str(row['ori_dr_amt'])
            bal_due = str(row['bal_due'])
            try:
                mysql_cursor.execute("INSERT INTO outstandings (out_standing_id, bill_no, bill_dt, customer_id, v_type, code, due_dt, ori_dr_amount, bal_due) values('"+out_standing_id+"', '"+bill_no+"', '"+bill_dt+"', '"+customer_id+"', '"+v_type+"', '"+code+"', '"+due_dt+"', '"+ori_dr_amount+"', '"+bal_due+"') ON DUPLICATE KEY UPDATE out_standing_id = '"+out_standing_id+"', bill_no = '"+bill_no+"', bill_dt = '"+bill_dt+"', customer_id = '"+customer_id+"', v_type = '"+v_type+"', code = '"+code+"', due_dt = '"+due_dt+"', ori_dr_amount = '"+ori_dr_amount+"', bal_due = '"+bal_due+"'")
                mysql_conn.commit()
            except mysql.Error as err:
                res.append("Something went wrong: {}".format(err)) 
                f = open("log.txt", "a")
                f.write('\n'+str(err) +'\n'+str(row))
                f.close()
                f = open("log_file.txt", "a")
                f.write('\n--------------------- \n'+str(err)+'\n ----------------------\n'+str(row)+'\n-------------\n')
                f.close() 
                print("==============================")
                print(err)
                print(row)
                print("==============================")
        print("data inserted to outstanding table")
        f = open("log_file.txt", "a")
        f.write('\n'+'data inserted to outstanding table'+ '\t : Date and Time :'+str(now)+'\n')
        f.close()

except (Exception, psycopg2.Error):
    res.append("Something went wrong main try") 
     


# closing database connection.
'''if pg_conn:
    pg_cursor.close()
    pg_conn.close()
if mysql_conn:
    mysql_cursor.close()
    mysql_conn.close()'''

