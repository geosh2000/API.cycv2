<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Lists extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
  }

  public function chanGroup_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('chanGroups')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }
    
    public function tipoRsva_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('config_tipoRsva')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }
    
    public function itemTypes_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('itemTypes')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }   
    
    public function branchId_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'] == 'branchId' ? 'branchid' : 'cityForListing';
            
            switch( $data['field'] ){
                case 'branchId':
                    $this->db->select("branchid as id, PDV as name");
                    break;
                case 'Localidad':
                    $this->db->select("cityForListing as id, cityForListing as name");
                    break;
            }
            
            $this->db->from('PDVs')
                ->group_by($searchField)
                ->order_by('PDV');   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'] == 'Localidad' ? 'cityForListing' : $info['name'] , $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

    public function pdvSuper_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $query = "SELECT 
                FINDSUPPDVDAY(id, CURDATE(), 0) AS id,
                FINDSUPPDVDAY(id, CURDATE(), 2) AS name
            FROM
                PDVs
            GROUP BY name
            HAVING name IS NOT NULL ORDER BY name";
            
            if( $q = $this->db->query($query) ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', null);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

    public function listProfiles_get(){
        $this->db->select('id, profile_name as name')
            ->from('profilesDB')
            ->order_by('profile_name');

        if( $q = $this->db->get() ){
            okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
    }

    public function pdvList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $this->db->select("a.*,
                CONCAT(
                    CASE 
                        WHEN a.id = 137 THEN 'Contact Center'
                        WHEN a.id = 440 THEN 'Contact Center'
                        ELSE TRIM(displayNameShort)
                    END, ' - ', cityForListing) AS displayNameList", FALSE)
                ->from('PDVs a')
                ->join('cat_zones b', 'a.branchZoneId = b.id', 'left')
                ->where('Activo',1)
                ->or_where('a.id',137)
                ->or_where('a.id',440);
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', null);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

    public function depList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $calls = $this->uri->segment(4);
            
            $this->db->from('PCRCs')
                    ->order_by('Departamento');
            
            if( isset($pais) && $pais != 0 ){ 
                $this->db->where('sede', $pais);
            }

            if( isset($calls) ){ 
                $this->db->where('inbound_calls', $calls);
            }
                    
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function fDepList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $isVenta = $this->uri->segment(3);
            $isSoporte = $this->uri->segment(4);
            $pais = $this->uri->segment(5);
            
            $this->db->select("id,
                                sede, 
                                f_name,
                                CONCAT(sede,' - ',f_name) as displayName,
                                f_skin AS skin,
                                f_skout AS skout,
                                CONCAT('(', f_skills, ')') AS skill,
                                isVenta, isSoporte, isAgency,
                                f_marca AS marca,
                                f_mp AS mp")
                    ->from('PCRCs')
                    ->where('isTablaF', 1)
                    ->where('isSoporte', $isSoporte)
                    ->where('isVenta', $isVenta)
                    ->order_by('displayName');
            
            if( isset($pais) ){
                $this->db->where('sede', $pais);
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function pdvSupList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("asesor, NOMBREASESOR(asesor, 1) AS Nombre, NOMBREASESOR(asesor, 2) AS NombreCompleto")
                    ->from('dep_asesores a')
                    ->join('PCRCs b', 'a.dep = b.id', 'left')
                    ->where('Fecha', $fecha)
                    ->where('sede', $pais)
                    ->where_in('puesto', array(11,48))
                    ->where('vacante IS NOT ', 'NULL', FALSE)
                    ->where_in('dep', array(29,56))
                    ->order_by('Nombre');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function pdvZoneList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            
            $this->db->select("*")
                    ->from('pdv_zonesCustom')
                    ->where('pais', $pais)
                    ->where('active', 1)
                    ->order_by('nombreZona');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function ccSupList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("asesor, NOMBREASESOR(asesor, 1) AS Nombre, NOMBREASESOR(asesor, 2) AS NombreCompleto")
                    ->from('dep_asesores a')
                    ->join('PCRCs b', 'a.dep = b.id', 'left')
                    ->where('Fecha', $fecha)
                    ->where('sede', $pais)
                    ->where_in('puesto', array(11,17,18,19,20,21,37,38,39,40,41,42,45,48))
                    ->where('vacante IS NOT ', 'NULL', FALSE)
                    ->where('(dep NOT IN (29,56) OR (dep = 29 AND puesto IN (17,48)))', null, FALSE)
                    ->order_by('Nombre');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function pdvModuleList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("a.id,
                                PDV,
                                TRIM(displayNameShort) as displayNameShort,
                                b.Ciudad,
                                cityForListing,
                                FINDSUPERDAYPDV('$fecha', a.id, 3) AS Supervisor,
                                FINDSUPERDAYPDV('$fecha', a.id, 2) AS SupervisorName,
                                customZone")
                    ->from('PDVs a')
                    ->join('cat_zones b', 'a.ciudad = b.id', 'left')
                    ->group_start()
                        ->where('Activo', 1)
                        ->or_where('PDV LIKE', "'%General%'", FALSE)
                    ->group_end()
                    ->where('pais', $pais)
                    ->order_by('displayNameShort');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function pdvMetas_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $mes = $this->uri->segment(4);
            $anio = $this->uri->segment(5);
            $status = $this->uri->segment(6);

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS plazas");
            $this->db->query("CREATE TEMPORARY TABLE plazas SELECT 
                                    oficina, COUNT(*) AS plazas
                                FROM
                                    asesores_plazas pl
                                WHERE
                                    pl.Status = 1 AND pl.Activo = 1
                                        AND pl.fin > LAST_DAY(CONCAT('$anio-$mes-01'))
                                        AND puesto NOT IN (17,11) 
                                GROUP BY oficina");
            $this->db->query("ALTER TABLE plazas ADD PRIMARY KEY (oficina)");
            
            $this->db->select("a.id,
                                a.PDV,
                                TRIM(displayNameShort) as displayNameShortOk,
                                a.Activo,
                                b.Ciudad,
                                plazas,
                                $mes as mes,
                                $anio as anio,
                                meta_total,
                                meta_hotel,
                                meta_total_diaria,
                                meta_hotel_diaria")
                    ->from('PDVs a')
                    ->join('cat_zones b', 'a.ciudad = b.id', 'left')
                    ->join('metas_pdv m', "a.id=m.pdv AND m.mes=$mes AND m.anio=$anio", 'left')
                    ->join('plazas pl', "a.id=pl.oficina", 'left')
                    ->where('pais', $pais)
                    ->order_by('Ciudad')
                    ->order_by('displayNameShortOk');
            
            switch($status){
                case 1:
                    $this->db->where('a.Activo',1);
                    break;
                case 2:
                    $this->db->where('a.Activo',0);
                    break;
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function ccAsesoresSupList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("a.asesor,
                                TRIM(NOMBREASESOR(a.asesor,2)) as displayNameShort,
                                b.Departamento,
                                color,
                                NOMBREPUESTO(puesto) as puesto,
                                FINDSUPERDAYCC('$fecha', a.asesor, 3) AS Supervisor,
                                FINDSUPERDAYCC('$fecha', a.asesor, 2) AS SupervisorName")
                    ->from('dep_asesores a')
                    ->join('PCRCs b', 'a.dep=b.id', 'left')
                    ->where('vacante IS NOT ', "NULL", FALSE)
                    ->where('sede', $pais)
                    ->where('Fecha', $fecha)
                    ->where('puesto !=', 20)
                    ->where('(dep NOT IN (29,56,47) OR (dep=29 AND puesto IN (11,48)))', null, FALSE)
                    ->order_by('displayNameShort');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function ofertas_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();

            foreach($data as $index => $info){
                $data[$index]['creator'] = $_GET['usid'];

                if( !isset($data[$index]['Activo']) ){
                    $data[$index]['Activo'] = 1;
                }

                if( !isset($data[$index]['Incentivo']) ){
                    $data[$index]['Incentivo'] = 0;
                }
            }

            if( $this->db->insert_batch('pdv_ofertas', $data) ){
                
                okResponse( 'Info Obtenida', 'data', TRUE, $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function ofertas_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $fecha = $this->uri->segment(3);
            
            $this->db->select("*, COALESCE(incentivo_pdv,0)+COALESCE(incentivo_cc,0) as inc")
                    ->from('pdv_ofertas')
                    ->order_by('inc DESC')
                    ->order_by('Destination')
                    ->order_by('Name');

            if( isset($fecha) ){
                $this->db->where("'$fecha' BETWEEN bookWinStart AND bookWinEnd", NULL, FALSE);
            }else{
                $this->db->where("CURDATE() BETWEEN bookWinStart AND bookWinEnd", NULL, FALSE);
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function updateOfertas_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            
            $this->db->set($data['field'], $data['val'])
                    ->set('last_editor', $_GET['usid'])
                    ->where('id', $data['id']);

            if( $this->db->update('pdv_ofertas') ){
                
                okResponse( 'Info Obtenida', 'data', true, $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function updateMasBuscados_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            
           

            if( $data['delete'] ){
                if( $this->db->where('id', $data['id'])->delete('t_masBuscados') ){
                    
                    okResponse( 'Info Borrada', 'data', true, $this);
                    
                }else{
                    errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            }else{
                $this->db->set($data['field'], $data['val'])
                    ->where('id', $data['id']);
                if( $this->db->update('t_masBuscados') ){
                    
                    okResponse( 'Info Obtenida', 'data', true, $this);
                    
                }else{
                    errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            }        

            return true;
        });
      
    }

    public function addMasBuscados_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            
           

            $this->db->set($data);

            if( $this->db->insert('t_masBuscados') ){
                
                okResponse( 'Info Obtenida', 'data', true, $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
              

            return true;
        });
      
    }


    public function pdvZoneCoordList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("a.id,
                                nombreZona as PDV,
                                nombreZona as displayNameShort,
                                FindCoordDayPDV('$fecha', a.id, 3) AS Supervisor,
                                FindCoordDayPDV('$fecha', a.id, 2) AS SupervisorName")
                    ->from('pdv_zonesCustom a')
                    ->where('active', 1)
                    ->where('pais', $pais)
                    ->order_by('nombreZona');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }


    public function pdvCoordList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $fecha = $this->uri->segment(4);
            
            $this->db->select("asesor, NOMBREASESOR(asesor, 1) AS Nombre, NOMBREASESOR(asesor, 2) AS NombreCompleto")
                    ->from('dep_asesores a')
                    ->join('PCRCs b', 'a.dep = b.id', 'left')
                    ->where('Fecha', $fecha)
                    ->where('sede', $pais)
                    ->where_in('puesto', array(11,17,48))
                    ->where('vacante IS NOT ', 'NULL', FALSE)
                    ->where_in('dep', array(29,56))
                    ->order_by('Nombre');
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function individualZone_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            $name = $data['name']['name'];
            // okResponse( 'Info Obtenida', 'data', $name, $this);
            
            $query = "SELECT nombreZona, FINDCOORDDAYPDV(CURDATE(), id, 2) AS Coord
                            FROM
                                pdv_zonesCustom
                            WHERE
                                active = 1
                            HAVING Coord = '$name'";
            
        
            if( $q = $this->db->query($query) ){
                
                okResponse( 'Info Obtenida', 'data', $q->row_array(), $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

    public function monitorSkills_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            
            $query = "SELECT 
                        CONCAT(monShow,'_',direction) as skill,
                        CONCAT(NOMBREDEP(monShow),
                                '  ',
                                IF(direction = 1,
                                    '(Inbound)',
                                    '(Outbound)')) AS name,
                        direction,
                        CONCAT('[',
                                GROUP_CONCAT('\"', queue, '\"'),
                                ']') AS qs,
                        CONCAT('{',
                                GROUP_CONCAT('\"', queue, '\":\"', shortName, '\"'),
                                '}') AS nameQs
                    FROM
                        Cola_Skill a
                            LEFT JOIN
                        PCRCs b ON a.monShow = b.parentId
                    WHERE
                        sede = '$pais' AND parentID = b.id
                    GROUP BY monShow , direction
                    HAVING name IS NOT NULL";
            
        
            if( $q = $this->db->query($query) ){

                $result = array();

                foreach( $q->result_array() as $index => $info ){
                    array_push($result,array(
                        'skill' => $info['skill'],
                        'name'  => $info['name'],
                        'qs'    => json_decode($info['qs']),
                        'nameQs'=> json_decode($info['nameQs'],true)
                    ));
                }
                
                okResponse( 'Info Obtenida', 'data', $result, $this);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
    }

}
