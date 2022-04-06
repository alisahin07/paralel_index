import os

import cx_Oracle


from pythonKodu.komuta_merkezi import *

def row_dondur2(a):
    dizi = []
    c.execute(a)

    for row in c:
        d = {}
        d['script'] = row[0]
        d['index_name']=row[1]

        dizi.append(d)
    return dizi

def  sorgu():
    a = '''    
select TO_CHAR (SUBSTR (dbms_metadata.get_ddl('INDEX', index_name, owner), 1, 4000))   as scriptt,index_name
from all_indexes 
where owner in ('{}')  
           '''.format(kullanici)

    return row_dondur2(a)


def sadece_kodu_update(nn):
    yazi = ''
    script=nn['script']
    index_name=nn['index_name']
    script.find("PCTFREE")
    new_index=script[:script.find("PCTFREE")-1]+'  PARALLEL 3 NOLOGGING; '
    new_index=new_index+' \n  alter index {}.{} noparallel; '.format(kullanici,index_name)

    sql = new_index
          

    yazi = yazi + sql + '\n\n'

    yazdir(yazi)


def yazdir(yazi):
    file1 = open(dosya_yolu, "a")
    # \n is placed to indicate EOL (End of Line)
    file1.write("  \n")
    file1.writelines(yazi)
    file1.close()  # to change file access modes
    print(yazi)
def sil(dosyaa):
    if os.path.exists(dosyaa):  # FENER_KOLONLARI_OLUSTUR.sql dosyasi siliniyor
        os.remove(dosyaa)
    else:
        print("The file does not exist")

def kontrol(sorgu1):
    for nn in sorgu1:
            sadece_kodu_update(nn)


user = merkez_user1
# parola_gir = 'hastane'  # input("veritabani_parola_gir=")
parola_gir = merkez_parola
dbname = merkez_sidname  # input("db_name_gir=")
# host_name = '192.168.1.100'  # input("hostname_giriniz.orn=DESKTOP-4H3IFJT=")192.168.1.100
host_name = merkez_ip  # DESKTOP-O3PV3T4 localhost   BAKIRKOY->>172.16.94.6
# kullanici='HASTANE'
kullanici = merkez_kullanici1
milyon = '+' + '1000000'
update_edilen_tablolar = 'update_edilen_tablolar'
tirnak2 = '"'
kolon_isimlendir = 'FENER_'
uniq_hastane = 'HASTANE_NO1'
dsn_tns = cx_Oracle.makedsn(host_name, 1521, dbname)
con = cx_Oracle.connect(user, parola_gir, dsn_tns, mode=cx_Oracle.SYSDBA)  # , mode=cx_Oracle.SYSDBA
c = con.cursor()
dosya_yolu = r"C:\Users\toshıba\Desktop\Paralel_index\outputt\paralel_index.sql"
sil(dosya_yolu)
kontrol(sorgu())




c.close()
con.close()