import rpa as r

def explorer_info(user_id, user_pwd, rpa_info):
    
    r.init()
    # earthdata login
    r.url('https://urs.earthdata.nasa.gov/home')
    if (r.exist('//input[@name="commit"]')):
        r.type('//input[@id="username"]', '[clear]' + rpa_info['earthdata_id'])
        r.type('//input[@id="password"]', '[clear]' + rpa_info['earthdata_pwd'])    
        r.click('//input[@name="commit"]')   
    
    r.dom('document.body.insertAdjacentHTML("afterbegin","<a href=\'https://earthexplorer.usgs.gov\' target=\'_blank\' id=\'earthexplorer\'>earthexplorer</a>");')
    r.click('earthexplorer')
    r.popup('earthexplorer.usgs.gov')
    r.wait(3)
    
    # earthexplorer login
    if (r.exist('//a[@href="/login"]')):
        r.click('//div[@id="navbar-menu"]//ul[3]//li[3]//a')
        r.wait(3)
        r.type('//input[@placeholder=" Registered USGS Username"]', user_id)
        r.type('//input[@placeholder="Registered USGS Password"]', user_pwd)
        r.click('//input[@id="loginButton"]')
        r.wait(3)

    # select tab1
    r.click('//div[@id="tab1"]')

    if rpa_info['area'] == 'tabCircle':
        
        # circle button
        r.click('//div[@id="tabCircle"]')

        # type center lat, lon, radius 
        r.type('//*[@id="centerLat"]', '[clear]' + rpa_info['centerLat'])
        r.type('//*[@id="centerLng"]', '[clear]' + rpa_info['centerLng'])
        r.select('//select[@id="unitType"]', rpa_info['unitType'])
        r.type('//*[@id="circleRadius"]', '[clear]' +  rpa_info['circleRadius'])

        # click apply button
        r.click('//input[@id="circleEntryApply"]')

    # set date range
    r.type('//*[@id="start_linked"]', '[clear]' + rpa_info['start_linked'])
    r.type('//*[@id="end_linked"]', '[clear]' + rpa_info['end_linked'])

    # click Data Sets button
    r.click('//div[@class="tabButtonContainer tabButtons"]//input[@title="Data Sets"]')
    
    # click Data Sets button
    r.click('//span[@id="refreshDatasetList"]')

    # select dataset
    for item  in rpa_info['dataset']: 
        r.click(item)
        if (r.exist('//a[normalize-space()="NASA Earthdata Login Credentials"]')):
            r.click('//button[normalize-space()="OK"]')              
            
        # r.click('NASA LPDAAC Collections')
        # r.click('MODIS Net Evapotranspiration - V6')
        # r.click('//label[@id="collLabel_5e83a6597a8ef6ef"]')
        # r.click('//label[@id="collLabel_5e83dde77d34bdf5"]')

    # additional criteria
    r.click('//div[@name="dataSetForm"]//input[@title="Additional Criteria"]')

    # click Result button
    r.click('//div[@id="tab3data"]//input[@title="Results"]')
    
    result_max = r.dom('return document.querySelector("#pageSelector_5e83a6597a8ef6ef_H").getAttribute("max")')    
    #dom_result2 = r.dom('return document.getElementById("pageSelector_5e83a6597a8ef6ef_H").getAttribute("max")')
    if int(result_max) > 1:
        for i in range(1, int(result_max) +1):
            r.table('//table[@class="resultPageTable"]', 'result_' + str(i) +'.csv')
            # click Result button
            r.click('Next ›')
            #r.click('(//a[@id="' + str(i+1) + '_5e83a6597a8ef6ef"])[1]')
    else:
        r.table('//table[@class="resultPageTable"]', 'result_' + 1 +'.csv')
    
    # Save a screenshot of the web page to top_result.png
    r.snap('page', 'earthexplorer_result.png')
    r.close()
    


if __name__ == "__main__":
    print("Satellite Dataset Scraper Start!")

    pg_con_info = {'host': '192.168.123.132', 'dbname': 'water',
                   'user': 'postgres', 'password': 'pispdb2021', 'port': 5432}
    
    # area tab { tabPolygon, tabCircle, tabPredefinedArea}
    rpa_info = {'area': 'tabCircle', 'centerLat': '38', 'centerLng' : '128', 'unitType' : 'km', 'circleRadius' : '500',
                           'start_linked': '01/01/2022', 'end_linked': '02/01/2022', 'dataset' : ['NASA LPDAAC Collections','MODIS Net Evapotranspiration - V6','MODIS MOD16A2 V6'],
                           'earthdata_id' : 'gp_ymseo', 'earthdata_pwd' : 'GPseo4655'}
    
    explorer_info('ymseo', 'geopeopleseo4655', rpa_info)
    
    print("Satellite Dataset Scraper End!")