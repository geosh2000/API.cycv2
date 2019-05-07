<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Prorep extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function locs_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $params = $this->put();
        $data = $params['data'];
        $gen = $params['gen'];   
        
        $this->db->query("SET @inicio = CAST('".$data['fecha']['Fecha']['params'][0]."' as DATE)");
        $this->db->query("SET @fin = CAST('".$data['fecha']['Fecha']['params'][1]."' as DATE)");
        $this->db->query("SET @h_inicio = CAST('".$data['fecha']['Hora']['params'][0]."' as TIME)");
        $this->db->query("SET @h_fin = CAST('".$data['fecha']['Hora']['params'][1]."' as TIME)");

        $this->db->query("SET @isPaq = ".($gen['isPaq'] == true ? "1" : "0"));
        $this->db->query("SET @outlet = ".($gen['outlet'] == true ? "1" : "0"));
        $this->db->query("SET @sv = ".($gen['sv'] == true ? "1" : "0"));
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS allLocs");
        $this->db->query("CREATE TEMPORARY TABLE allLocs SELECT * FROM t_Locs WHERE Fecha BETWEEN @inicio AND @fin");
        $this->db->query("ALTER TABLE allLocs ADD PRIMARY KEY (`Localizador`, `Venta`, `Fecha`, `Hora`)");
        $this->db->query("INSERT INTO allLocs SELECT * FROM (SELECT * FROM t_Locs WHERE Fecha BETWEEN @inicio AND @fin) a ON DUPLICATE KEY UPDATE Venta=a.Venta");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsRAW");
        $this->db->query("CREATE TEMPORARY TABLE locsRAW SELECT
                            Localizador,
                            a.asesor,
                            a.Nombre as asName,
                            d.dep, d.oficina,
                            chanId,
                            a.branchId,
                            p.id as pdvId,
                            SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                            IF(SUM(VentaMXN) > 0, Localizador, NULL) AS NewLoc,
                            IF(SUM(VentaMXN) > 0
                                    AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                Localizador,
                                NULL) AS CountLoc,
                            IF(SUM(VentaMXN) > 0
                                    OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                Localizador,
                                NULL) AS ModifLoc,
                            gpoCanal,
                            gpoCanalKpi,
                            canal,
                            tipoCanal,
                            marca,
                            pais,
                            p.branchid as branch_id,
                            p.PDV as name,
                            p.cityForListing as Localidad,
                            p.outlet,
                            tipoRsva,
                            gpoTipoRsva, a.Servicios
                        FROM
                            allLocs a
                                LEFT JOIN
                            chanGroups b ON a.chanId = b.id
                                LEFT JOIN
                            PDVs p ON a.branchId = p.branchId
                                LEFT JOIN
                            dep_asesores d ON a.asesor = d.asesor
                                AND a.Fecha = d.Fecha
                                LEFT JOIN
                            config_tipoRsva e ON IF(a.tipo IS NULL OR a.tipo = '',
                                0,
                                a.tipo) = e.tipo
                                AND IF(d.dep IS NULL,
                                IF(a.asesor = - 1, - 1, 0),
                                IF(d.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                    0,
                                    d.dep)) = e.dep
                        GROUP BY Localizador");
        $this->db->query("ALTER TABLE locsRAW ADD PRIMARY KEY (Localizador)");
        
        $this->db->query("DROP TABLE IF EXISTS destinations");
        $this->db->query("CREATE TABLE destinations
                        SELECT 
                            Localizador, Destination
                        FROM
                            t_hoteles_test
                        WHERE
                            Destination != '' AND
                            Fecha BETWEEN @inicio AND @fin
                        GROUP BY
                            Localizador");
        $this->db->query("ALTER TABLE destinations ADD PRIMARY KEY (Localizador)");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS itemsRAW");
        $this->db->query("CREATE TEMPORARY TABLE itemsRAW SELECT
                            a.*,
                            asesor,
                            asName,
                            dep,
                            Monto,
                            NewLoc,
                            CountLoc,
                            ModifLoc,
                            gpoCanal,
                            gpoCanalKpi,
                            canal,
                            tipoCanal,
                            marca,
                            pais,
                            name AS branchName,
                            Localidad,
                            FINDSUPPDVDAY(IF(gpoCanalKpi != 'PDV', oficina, pdvId), CURDATE(), 0) as SupervisorId,
                            outlet,
                            IF(@outlet=1, IF(outlet=1, 'Outlet', tipoRsva), tipoRsva) as tipoRsva,
                            IF(@outlet=1, IF(outlet=1, 'Outlet', gpoTipoRsva), gpoTipoRsva) as gpoTipoRsva,
                            IF(@isPaq = 1, IF(itemLocatorIdParent != '','Paquete',servicio) ,servicio) as servicio,
                            Servicios as MLServices,
                            d.Destination as Destino,
                            HOUR(Hora) AS HG
                        FROM
                            t_hoteles_test a
                                LEFT JOIN
                            locsRAW b ON a.Localizador = b.Localizador
                                LEFT JOIN
                            itemTypes c ON a.itemType = c.type
                                AND a.categoryId = c.category
                                LEFT JOIN
                            destinations d ON a.Localizador=d.Localizador
                        WHERE
                            Fecha BETWEEN @inicio AND @fin
                                AND Hora BETWEEN @h_inicio AND @h_fin");

        $this->db->query("ALTER TABLE itemsRAW ADD PRIMARY KEY (`Localizador`, `Venta`, `Fecha`, `Hora`, `item`)");
        
        
        if( $gen['loc'] == 'true' ){
            $this->db->select("Localizador, MLServices, Destino")->group_by("Localizador")->having("Monto !=", 0);
        }
        
        foreach( $data as $group => $info1 ){
            foreach( $info1 as $field => $info2 ){
                if($info2['showCol']){
                    $f_name = $info2['name'] == 'Hora' ? 'HG' : $info2['name'];
                    $pars = count($info2['params']) == 0 ? 'Todo' : str_replace('`', '', implode(",",$info2['params']));
                    
                    switch($f_name){
                        case 'asesor':
                            if($info2['groupBy']){
                                $this->db->select($f_name);
                                $this->db->select("IF(asesor=0, asName, IF(asesor = -1, 'Online', NOMBREASESOR(asesor, 2))) AS Nombre");
                            }else{
                                $this->db->select("'$pars' as $f_name");
                            }
                            
                            break;
                        case 'SupervisorId':
                            if($info2['groupBy']){
                                $this->db->select("IF(SupervisorId IS NULL, 'N/A', NOMBREASESOR(SupervisorId, 2)) AS Supervisor");
                            }else{
                                $this->db->select("'$pars' as $f_name");
                            }
                            
                            break;
                        case 'Hora':
                            $this->db->select($f_name);
                            $this->db->select("Hora");
                            break;
                        case 'branchId':
                            if($info2['groupBy']){
                                $this->db->select($f_name);
                                $this->db->select("branchName");
                            }else{
                                $this->db->select("'$pars' as $f_name");
                            }
                            break;
                        case 'Localidad':
                            if($info2['groupBy']){
                                $this->db->select($f_name);
                                $this->db->select("Localidad");
                            }else{
                                $this->db->select("'$pars' as $f_name");
                            }
                            break;
                        case 'Fecha':
                            if( !$info2['groupBy' ] ){
                                $this->db->select("'".$info2['params'][0]."' as Inicio, '".$info2['params'][1]."' as Fin");
                            }else{
                                $this->db->select($f_name);
                            }
                            break;
                        default:
                            if($info2['groupBy']){
                                $this->db->select($f_name);
                            }else{
                                $this->db->select("'$pars' as $f_name");
                            }
                            
                            break;
                    }
                    
                }
                
                if($info2['groupBy']){
                    switch($f_name){
                        case 'asesor':
                            $this->db->group_by('Nombre');
                            break;
                        default:
                            $f_name = $info2['name'] == 'Hora' ? 'HG' : $info2['name'];
                            $this->db->group_by($f_name);
                            break;
                    }
                }
                
                if( count($info2['params']) > 0 ){
                    switch( $info2['searchType'] ){
                        case 'between':
                            $this->db->where($info2['name']." BETWEEN '", $info2['params'][0]."' AND '".$info2['params'][1]."'", FALSE);
                            break;
                        case 'in':
                            $this->db->where_in($info2['name'], $info2['params']);
                            break;
                    }
                }
            }
        }
        
        $this->db->select("SUM(IF(@sv = 1,
                                IF(NewLoc IS NOT NULL,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0),
                                VentaMXN + OtrosIngresosMXN + EgresosMXN)) AS Monto,
                            SUM(IF(@sv = 1,
                                IF(NewLoc IS NOT NULL, clientNights, 0),
                                clientNights)) AS RN,
                            COUNT(DISTINCT CountLoc) as Locs", FALSE)
            ->from('itemsRAW');
        
        
        $query = $this->db->get_compiled_select();
        
      if( $q = $this->db->query($query) ){
          
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this, 'query', $params );

      }else{

        errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function pr_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $params = $this->put();
        $data = $params['data'];
        $gen = $params['gen'];  

        $inicio = $params['data']['fecha']['Fecha']['params'][0];
        $fin = $params['data']['fecha']['Fecha']['params'][1];
        
        $this->db->select("a.*, canal, gpoCanal, IF(COALESCE(br.outlet,0)=1,'Outlet',gpoCanalKpi) as gpoCanalKpiOk, marca, pais, tipoCanal, c.dep, vacante, puesto, cc, ml.tipo")
            ->select("IF(CAST(dtCreated as DATE) = a.Fecha, Localizador, null) as NewLoc, CAST(dtCreated as DATE) as dtCreated", FALSE)
            ->select('ml.asesor')
            ->select('i.Servicio,tipoRsva, gpoTipoRsva,br.id as pdvId')
            ->select('NOMBREASESOR(ml.asesor,2) as NombreAsesor')
            ->from('t_hoteles_test a')
            ->join("t_masterlocators ml", "a.Localizador = ml.masterlocatorid", "left")          
            ->join("chanGroups b", "a.chanId = b.id", "left")
            ->join("PDVs br", 'a.branchId = br.branchId')
            ->join("dep_asesores c", "ml.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
            ->join("cc_apoyo d", "ml.asesor = d.asesor AND a.Fecha BETWEEN d.inicio AND d.fin", "left")
            ->join("itemTypes i", "a.itemType = i.type AND a.categoryId = i.category", "left", FALSE)
            ->join("config_tipoRsva tr", "IF(ml.tipo IS NULL OR ml.tipo='',0, ml.tipo) = tr.tipo
                                                AND IF(c.dep IS NULL,
                                                IF(ml.asesor <= - 1, - 1, 0),
                                                IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    c.dep)) = tr.dep", "left", FALSE);
        
        if( $params['data']['fecha']['Hora']['groupBy'] ){
            $this->db->select("CONCAT(HOUR(Hora),':',IF(MINUTE(Hora)>=30,30,'00'),':00') as HoraOK");
        }

        $fields = array(
            'branchId'  =>  'a.branchId',
            'canal'     =>  'a.chanId',
            'gpoCanal'  =>  'b.gpoCanal',
            'gpoCanalKpi' => "IF(COALESCE(br.outlet,0)=1,'Outlet',gpoCanalKpi)",
            'marca'     =>  'b.marca',
            'pais'      =>  'b.pais',
            'tipoCanal' =>  'b.tipoCanal',
            'Fecha'     =>  'a.Fecha',
            'Hora'      =>  'a.Hora',
            'Asesor'    =>  'ml.asesor',
            'Stand'     =>  'br.PDV',
            'servicio'  =>  'i.servicio',
            'tipoRsva'  =>  'tr.tipoRsva',
            'gpoTipoRsva'  =>  'gpoTipoRsva',
            'Hotel' => 'Hotel',
            'Corporativo' => 'Corporativo',
            'Destination' => 'Destination'
        );

        foreach($params['data'] as $g => $dg){
            foreach($dg as $f => $df){
                if( count($df['params']) > 0 ){
                    switch($df['searchType']){
                        case 'in':
                            $this->db->where_in($fields[$f],$df['params']);
                            break;
                        case 'like':
                            if(trim($df['params'][0]) != ''){
                                $this->db->where($fields[$f]." LIKE ","'%".$df['params'][0]."%'", FALSE);
                            }
                            break;
                        case 'between':
                            if($f == 'Fecha'){
                                $this->db->group_start();
                                    $this->db->where($fields[$f]." BETWEEN", "'".$df['params'][0]."' AND '".$df['params'][1]."'", FALSE);
                                    if( $params['gen']['compare'] ){
                                        $this->db->or_where("a.Fecha BETWEEN ", "ADDDATE('$inicio',-364) AND ADDDATE('$fin',-364)", FALSE);
                                    }
                                $this->db->group_end();
                            }else{
                                $this->db->where($fields[$f]." BETWEEN", "'".$df['params'][0]."' AND '".$df['params'][1]."'", FALSE);
                            }
                            break;
                    }
                }
            }
        }     
        
        $tableLocs = $this->db->get_compiled_select();

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS base");
        $this->db->query("CREATE TEMPORARY TABLE base $tableLocs");

        $this->db->from('base');

        if( $params['gen']['sv'] ){
            $this->db->select('SUM(VentaMXN+OtrosIngresosMXN+IF(NewLoc IS NULL,0,EgresosMXN)) as Monto')
            ->select('SUM(IF(NewLoc IS NULL,0,COALESCE(clientNights,0))) as RN');
        }else{
            $this->db->select('SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Monto')
            ->select('SUM(COALESCE(clientNights,0)) as RN');
        }

        $fieldsShow = array(
            'branchId'  =>  array('branchId','branchId, NOMBREPDV(pdvId,1) as Sucursal'),
            'canal'     =>  array('chanId','chanId'),
            'gpoCanal'  =>  array('gpoCanal','gpoCanal'),
            'gpoCanalKpi' => array('gpoCanalKpiOk','gpoCanalKpiOk'),
            'marca'     =>  array('marca','marca'),
            'pais'      =>  array('pais','pais'),
            'tipoCanal' =>  array('tipoCanal','tipoCanal'),
            'Fecha'     =>  array('Fecha','Fecha'),
            'Hora'      =>  array($params['data']['fecha']['Hora']['groupBy'] ? 'HoraOK' : 'Hora',$params['data']['fecha']['Hora']['groupBy'] ? 'HoraOK as Hora' : 'Hora'),
            'Asesor'    =>  array('asesor','asesor, NombreAsesor'),
            // 'Stand'     =>  array('PDV','PDV'),
            'servicio'  =>  array('servicio','servicio'),
            'tipoRsva'  =>  array('tipoRsva','tipoRsva'),
            'gpoTipoRsva'  =>  array('gpoTipoRsva','gpoTipoRsva'),
            'Hotel'       => array('HotelOk','HotelOk'),
            'Corporativo' => array('CorporativoOk','CorporativoOk'),
            'Destination' => array('DestinationOk','DestinationOk')
        );

        // if( $params['gen']['hotel'] ){
            
        // }

        foreach($params['data'] as $g => $dg){
            foreach($dg as $f => $df){
                if( $df['groupBy'] ){
                    $this->db->group_by($fieldsShow[$f][0]);
                }
                if( $df['showCol'] ){
                    if($fieldsShow[$f][1] == 'servicio' && !$df['groupBy'] ){
                        $this->db->select("GROUP_CONCAT(DISTINCT ".$fieldsShow[$f][1].") as servicio");
                    }elseif($f == 'Hotel' || $f == 'Corporativo' || $f == 'Destination'){
                        if( $df['groupBy'] ){
                            $this->db->select("$f as $f"."Ok");
                        }else{
                            $this->db->select("GROUP_CONCAT(DISTINCT $f) as ".$fieldsShow[$f][1]);
                        }
                    }else{
                        $this->db->select($fieldsShow[$f][1]);
                    }
                }
            }
        } 

        if( $params['gen']['loc'] ){
            $this->db->select('Localizador')
            ->group_by('Localizador');
        }

        if( $q = $this->db->get() ){
            okResponse( 'Información Obtenida', 'data', $q->result_array(), $this, 'query', $params );
        }else{
            errResponse('Error al compilar tabla base', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        

    });

  }
    
    public function savePreset_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        $params = $data['params'];
        $name   = $data['name'];

        $this->db->set(array( 'val' => $params, 'name' => $name, 'asesor' => $_GET['usid']));
        
        
        if( $this->db->insert('config_proRepPresets') ){

            okResponse( 'Preset Guardado', 'data', true, $this );

        }else{

            errResponse('Error al guardar preset', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }


      return true;

    });

    jsonPrint( $result );

  } 
    
    public function loadPreset_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $this->db->select('*')
            ->from('config_proRepPresets')
            ->where_in('asesor', array(0, $_GET['usid']));
        
        
        if( $q = $this->db->get() ){

            okResponse( 'Preset Cargados', 'data', $q->result_array(), $this );

        }else{

            errResponse('Error al cargar preset', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }


      return true;

    });

    jsonPrint( $result );

  }


 
}
