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
        $this->db->query("INSERT INTO allLocs SELECT * FROM (SELECT * FROM d_Locs WHERE Fecha BETWEEN @inicio AND @fin) a ON DUPLICATE KEY UPDATE Venta=a.Venta");
        
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
                            c.branchId as branch_id,
                            c.name,
                            c.cityForListing as Localidad,
                            c.outlet,
                            tipoRsva,
                            gpoTipoRsva, a.Servicios
                        FROM
                            allLocs a
                                LEFT JOIN
                            chanGroups b ON a.chanId = b.id
                                LEFT JOIN
                            cat_branch c ON a.branchId = c.branchId
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
        
//        BUILD REPORT
        
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
