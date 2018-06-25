<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Headcount extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
  }

  public function codigosPuesto_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->temporaryPuestos();

      if($q = $this->db->query("SELECT * FROM cdPuestos")){
        $result = array(
                      "status"    => true,
                      "msg"       => $this->db->error(),
                      "rows"      => $q->num_rows(),
                      "data"     => $q->result_object()
                    );
      }else{
        $result = array(
                      "status"    => false,
                      "msg"       => $this->db->error(),
                      "data"      => null
                    );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function listPuestos_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->temporaryPuestos();

      $params = array(
                        "UDN" => $this->uri->segment(4),
                        "Area" => $this->uri->segment(5),
                        "Departamento" => $this->uri->segment(6),
                        "Puesto" => $this->uri->segment(7),
                      );

      foreach($params as $index => $value){
        if($value != null){
          $where[$index] = $value;
        }
      }

      if(!isset($where)){
        $where[1]=1;
      }

      $search = $this->uri->segment(3);

      $relates = array(
                        "Area"          => "UDN",
                        "Departamento"  => "Area",
                        "Puesto"        => "Departamento",
                        "Alias"         => "Puesto"
                      );

      if($search == 'UDN'){
        $search = "UDN !=";
        $param = 0;
        $this->db->select("UDN as id, UDN_nombre as name")
                  ->group_by('UDN');
      }else{
        $this->db->select("$search as id, ".$search."_nombre as name")
                  ->group_by( $search )
                  ->where( $where );
      }

      if($search == 'Alias' OR $search == 'alias'){
        $this->db->select("puestoID as id, Alias as alias_id, Departamento_id as dep_id, Codigo as codigo, pcrc, ".$search."_nombre as name");
      }

      $query = $this->db->get_compiled_select("cdPuestos");

      $q = $this->db->query($query);

      $result = array(
                      "status"    => true,
                      "msg"       => $this->db->error(),
                      "data"     => $q->result_array()
                    );

      return $result;

    });

    jsonPrint( $result );

  }

  public function listPdvs_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $q = $this->db->query("SELECT a.id, CONCAT('(',b.Ciudad,') ',PDV) as name, b.id as ciudad FROM PDVs a LEFT JOIN db_municipios b ON a.ciudad=b.id WHERE Activo=1 AND (PDV LIKE '%MX%' OR PDV LIKE '%GENERAL%') AND PDV NOT LIKE '%y-outlet%' AND b.Ciudad IS NOT NULL ORDER BY b.Ciudad, PDV");

        $result = array(
                        "status"    => true,
                        "msg"       => "listado completo",
                        "data"     => $q->result_array()
                      );

        return $result;

      });

      jsonPrint( $result );

  }

  private function temporaryPuestos(){
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS cdPuestos");
    $this->db->query("CREATE TEMPORARY TABLE cdPuestos SELECT
                  a.id as Alias, b.*, a.Puesto as Alias_nombre
              FROM
                  (SELECT
                      a.puesto as id, b.Puesto, hc_puesto
                  FROM
                      asesores_plazas a
                  LEFT JOIN PCRCs_puestos b ON a.puesto = b.id
                  WHERE
                      hc_puesto IS NOT NULL
                  GROUP BY a.puesto , hc_puesto) a
                      LEFT JOIN
                  (SELECT
                      a.id AS puestoID,
                          d.clave AS UDN,
                          c.clave AS Area,
                          b.clave AS Departamento,
                          a.clave AS Puesto,
                          CONCAT(d.clave, '-', c.clave, '-', b.clave, '-', a.clave) AS Codigo,
                          d.nombre AS UDN_nombre,
                          c.nombre AS Area_nombre,
                          b.nombre AS Departamento_nombre,
                          a.nombre AS Puesto_nombre,
                          d.id AS UDN_id,
                          c.id AS Area_id,
                          b.id AS Departamento_id,
                          a.id AS Puesto_id,
                          b.pcrc as pcrc
                  FROM
                      hc_codigos_Puesto a
                  LEFT JOIN hc_codigos_Departamento b ON a.departamento = b.id
                  LEFT JOIN hc_codigos_Areas c ON b.area = c.id
                  LEFT JOIN hc_codigos_UnidadDeNegocio d ON c.unidadDeNegocio = d.id) b ON a.hc_puesto = b.puestoID
                ORDER BY UDN_nombre , Area_nombre , Departamento_nombre , Puesto , Alias_nombre");
  }

  public function addVacante_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $cleanData = $this->put();

        $insert = array(
                        'main_dep'  => 1,
                        'hc_dep'    => $cleanData['alias']['dep_id'],
                        'hc_puesto' => $cleanData['alias']['id'],
                        'departamento' => $cleanData['alias']['pcrc'],
                        'puesto'    => $cleanData['alias']['alias_id'],
                        'oficina'   => $cleanData['oficina']['id'],
                        'ciudad'    => $cleanData['oficina']['ciudad'],
                        'inicio'    => $cleanData['inicio'],
                        'fin'       => $cleanData['fin'],
                        'esquema'   => $cleanData['esquema'],
                        'comentarios'  => $cleanData['comentarios'],
                        'Activo'    => 1,
                        'Status'    => 0,
                        'created_by'   => $cleanData['creador']
                      );

        if($cleanData['fin'] == null){
          $insert['fin'] = '2030-12-31';
        }

        for($i=1; $i<=$cleanData['cantidad'];$i++){
          $this->db->insert('asesores_plazas', $insert);
          $moves[] = array(
                            'vacante'     => $this->db->insert_id(),
                            'fecha_out'   => $cleanData['inicio']
                          );
          $mopers[] = $this->db->insert_id();
        }

        solicitudVacante::mail($this, array('vacante' => $mopers[0], 'cantidad' => $cleanData['cantidad'], 'status' => 0, 'applier' => $cleanData['creador']), 'ask');

        if($this->db->insert_batch('asesores_movimiento_vacantes', $moves)){
          $result = array(
                          "status"    => true,
                          "msg"       => "Inserts Listos",
                          "ids_vacantes"  => $mopers
                        );
        }else{
          $result = array(
                          "status"    => false,
                          "msg"       => $this->db->error(),
                          "ids_vacantes"  => null
                        );
        }

        return $result;

      });

      jsonPrint( $result );

  }

  public function downloadableList_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $paramTitle = array(
                            5 =>  'UDN_nombre',
                            6 =>  'Area_nombre',
                            7 =>  'Departamento_nombre',
                            8 =>  'Puesto_nombre'
                          );

      for($i=5; $i<=8; $i++){
        if($this->uri->segment($i) != null){
          $params[$paramTitle[$i]] = urldecode($this->uri->segment($i));
        }
      }

      if(!isset($params)){
        $params['UDN_nombre IS NOT NULL'] = null;
      }

      $this->temporaryPuestos();

      $this->db->select("a.id,
                          Codigo,
                          UDN_nombre AS UDN,
                          Area_nombre AS Area,
                          Departamento_nombre AS Departamento,
                          Puesto_nombre AS Puesto,
                          f.Puesto as Alias,
                          a.esquema AS Esquema,
                          d.PDV AS Oficina,
                          e.Ciudad,
                          a.inicio,
                          a.fin,
                          a.comentarios,
                          IF(a.Status = 1 AND asesor IS NULL,'Vacante',NULL) as Vacantes,
                          IF(CURDATE()<=a.fin,'Activa','Inactiva') as Activa,
                          CASE
                            WHEN a.Status=0 THEN 'Pendiente Aprobacion'
                            WHEN a.Status=1 THEN 'Aprobada'
                            WHEN a.Status=2 THEN 'Denegada'
                          END as Aprobacion,
                          NOMBREASESOR(a.created_by, 1) AS Creador,
                          NOMBREASESOR(approbed_by, 1) AS Aprobada_por,
                          CAST(date_created AS DATE) AS Fecha_creacion,
                          deactivated_by AS Desactivada_por,
                          date_deactivated AS Fecha_Desactivacion,
                          deactivation_comments as Comentarios_Desactivacion,
                          NOMBREASESOR(asesor, 2) AS Asesor_actual,
                          num_colaborador,
                          Ingreso")
                ->from('asesores_plazas a')
                ->join('(SELECT * FROM cdPuestos GROUP BY puestoID) b', 'a.hc_puesto = b.puestoID', 'LEFT')
                ->join('dep_asesores c', 'a.id = c.vacante AND c.Fecha = CURDATE()', 'LEFT')
                ->join('PDVs d', 'a.oficina = d.id', 'LEFT')
                ->join('db_municipios e', 'a.ciudad = e.id', 'LEFT')
                ->join('PCRCs_puestos f', 'a.puesto = f.id', 'LEFT')
                ->join('Asesores g', 'c.asesor = g.id', 'LEFT')
                ->where($params)
                ->order_by('Codigo');

      if($query = $this->db->get()){
        $result = array(
                        'status'  => true,
                        'msg'     => "Vacantes obtenidas",
                        'rows'    => $query->num_rows(),
                        'data'    => $query->result_object(),
                        'headers' => $query->list_fields()
                      );
      }else{
        $result = array(
                        'status'  => true,
                        'msg'     => $this->db->error(),
                        'rows'    => 0,
                        'data'    => null
                      );
      }



      return $result;

      });

      jsonPrint( $result );

  }

  public function deactivateVacante_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $update = array(
                      'fin' => $data['fecha'],
                      'deactivated_by' => $data['creador'],
                      'deactivation_comments' => $data['comments'],
                      'Activo' => 0,
                      'Status' => 2,
                    );
      $this->db->set('date_deactivated', "NOW()", FALSE)
                ->set($update)
                ->where("id = ".$data['id']);

      if($query = $this->db->update('asesores_plazas')){
        $result = array(
                        'status'  => true,
                        'msg'     => "Vacante ".$data['id']." desactivada correctamente",
                      );
      }else{
        $result = array(
                        'status'  => false,
                        'msg'     => $this->db->error()
                      );
      }

      return $result;

      });

      jsonPrint( $result );

  }

  public function test_get(){

    $result = solicitudVacante::mail($this, array('vacante' => 685, 'cantidad' => 4, 'status' => 1, 'applier' => 177), 'set');

    jsonPrint( $result );

  }

  public function deps_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $list = $this->db->query("SELECT id, Departamento FROM PCRCs WHERE parent_group=1 ORDER BY Departamento");
      $result = $list->result_array();

      return $result;

      });

      $this->response($result);
  }

  public function departamentos_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
      if( $list = $this->db->query("SELECT id, Departamento FROM PCRCs WHERE parent_group=1 ORDER BY Departamento") ){
        okResponse( 'Departamentos Obtenidos', 'data', $list->result_array(), $this );
      }else{
        errResponse( "Error en la base de datos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
      }

      return true;

    });

    $this->response($result);
  }

  //New Version
  public function FcodigosPuesto_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->temporaryPuestos();

      if($q = $this->db->query("SELECT * FROM cdPuestos")){

        okResponse( 'Puestos Obtenidos', 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    jsonPrint( $result );

  }

  public function listSupers_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select("asesor")
                ->select("NOMBREASESOR(asesor,2) as nombre ", FALSE)
              ->from('dep_asesores a')
              ->join('hc_codigos_Puesto b', 'a.hc_puesto = b.id', 'LEFT')
              ->where("(clave LIKE '%c%' OR clave LIKE '%b%')
                        AND Fecha = CURDATE() ", NULL, FALSE)
              ->order_by('nombre');
    
            $dep = $this->uri->segment(3);
        
            if( isset($dep) ){
                $this->db->where('dep', $dep );
            }

      if($q = $this->db->get()){

        okResponse( 'Supers Obtenidos', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    jsonPrint( $result );

  }
    
  public function Fdepartamentos_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select("a.id, a.pcrc, a.nombre as dep, b.nombre as area, c.nombre as udn")
              ->from('hc_codigos_Departamento a')
              ->join('hc_codigos_Areas b', 'a.area = b.id', 'LEFT')
              ->join('hc_codigos_UnidadDeNegocio c', 'b.unidadDeNegocio = c.id', 'LEFT')
              ->join('PCRCs d', 'a.pcrc = d.id', 'LEFT')
              ->where('parent_group', 1)
              ->order_by('udn, dep');

      if($q = $this->db->get()){

        okResponse( 'Puestos Obtenidos', 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    jsonPrint( $result );

  }

  public function getIdDepHc_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $query = "SELECT
                    a.id, CONCAT(c.nombre,' - ',b.nombre, ' - ', a.nombre) AS name
                FROM
                    hc_codigos_Departamento a
                        LEFT JOIN
                    hc_codigos_Areas b ON a.area = b.id
                        LEFT JOIN
                    hc_codigos_UnidadDeNegocio c ON b.unidadDeNegocio = c.id ORDER BY name";

      if($q = $this->db->query( $query )){

        okResponse( 'Deps Obtenidos', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    jsonPrint( $result );

  }
    

  public function photoList_get(){
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $pdv = $this->uri->segment(3);

          $this->db->select("NOMBREASESOR(asesor, 2) AS Nombre, NOMBREASESOR(asesor,5) as numColaborador,
                              Departamento AS depName", FALSE)
              ->from('dep_asesores a')
              ->join('PCRCs b', 'a.dep = b.id', 'left')
              ->where('Fecha =', 'CURDATE()', FALSE)
              ->where('vacante IS NOT ', 'NULL', FALSE)
              ->order_by('depName', 'Nombre');
          
          if( $pdv == 1 ){
              $this->db->where('dep', 29);
          }else{
              $this->db->where('dep !=', 29);
          }
          
          $title = array();
          $list = array();
          
          if( $q = $this->db->get() ){
              
              foreach( $q->result_array() as $index => $info ){
                  
                  if( $index == 0 ){
                      foreach( $info as $i => $tit ){
                          array_push( $title, $i );
                      }
                      array_push( $title, 'exists' );
                  }
                  
                  
                  
                  $tmp = $info;
                  
                  if(  $info['numColaborador'] != null &&  $info['numColaborador'] != '' ){
                      $tmp['exists'] = fExist('asesores', $info['numColaborador'], 'jpg' );
                  }else{
                      $tmp['exists'] = FALSE;
                  }
            
                  array_push( $list, $tmp );

                  
              }
              
              okResponse( 'Info Obtenida', 'data', array( 'data' => $list, 'titles' => $title ), $this, 'pdv', $pdv );
              
          }else{
              errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }

          return true;
      });
      jsonPrint( $result );
  }
  
  public function pdvAsesoresList_get(){
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $this->db->select("id, idCyc")
              ->from('dep_asesores a')
              ->join('adminUsersPT b', 'a.asesor = b.idCyc', 'left')
              ->where('Fecha =', 'CURDATE()', FALSE)
              ->where('dep', 29);   
          
          if( $q = $this->db->get() ){
              
              $result = array();
              
              foreach( $q->result_array() as $index => $info ){              
                  $result[$info['id']] = intVal($info['idCyc']);
              }
              
              okResponse( 'Info Obtenida', 'data', $result, $this);
              
          }else{
              errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }

          return true;
      });
      jsonPrint( $result );
  }

  public function asesoresList_put(){
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $filters = $this->put();
          
          $this->db->select("id, Nombre as name, Usuario")
              ->select("NOMBREDEP(b.dep) as Departamento")
              ->select("IF(dep=29,FINDSUPERDAYPDV(Fecha,oficina,2),FINDSUPERDAYCC(Fecha, asesor,2 )) as sup")
              ->select("IF(dep=29,FINDSUPERDAYPDV(Fecha,oficina,3),FINDSUPERDAYCC(Fecha, asesor,3)) as supId")
              ->from('Asesores a')
              ->join('dep_asesores b', 'a.id = b.asesor AND b.Fecha=CURDATE()', 'left', FALSE)
              ->where('vacante IS NOT ', 'NULL', FALSE)
              ->order_by('Nombre');   
          
          // Limit Results

            // UDN Filter
            if( $filters['udn'] != 0 ){
                $this->db->where_in('hc_udn', $filters['udn']);
            }          

            // area Filter
            if( $filters['area'] != 0 ){
                $this->db->where_in('hc_area', $filters['area']);
            }          

            // departamento Filter
            if( $filters['dep'] != 0 ){
                $this->db->where_in('hc_dep', $filters['dep']);
            }          

            // puesto Filter
            if( $filters['puesto'] != 0 ){
                $this->db->where_in('hc_puesto', $filters['puesto']);
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

  public function headCount_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $udn = $this->uri->segment(3);
        
      $this->db->query("CREATE TEMPORARY TABLE hc SELECT 
                pl.id,
                md.clave AS MainDep,
                hu.nombre AS UDN,
                ha.nombre AS Area,
                hd.nombre AS Departamento,
                hp.nombre AS Puesto,
                md.id AS MainDep_id,
                hu.id AS UDN_id,
                ha.id AS Area_id,
                hd.id AS Departamento_id,
                hp.id AS Puesto_id,
                pr.Departamento AS copcDep,
                pp.Puesto AS copcPuesto,
                CONCAT(hu.clave,'-',ha.clave,'-',hd.clave,'-',hp.clave) as Codigo,
                pdv.PDV AS Oficina,
                cd.Ciudad,
                inicio,
                fin,
                f_In as dtCubierta,
                f_Out as dtLiberada,
                esquema,
                NOMBREASESOR(das.asesor, 2) AS cubiertaPor,
                NOMBREASESOR(lib.asesor, 2) AS liberadaPor,
                pl.Activo,
                pl.Status,
                NOMBREASESOR(pl.approbed_by, 1) AS aprobadaPor,
                date_approbed,
                NOMBREASESOR(created_by, 1) AS CreadaPordor,
                date_created,
                NOMBREASESOR(deactivated_by, 1) AS desactivadaPor,
                date_deactivated,
                deactivation_comments
            FROM
                asesores_plazas pl
                    LEFT JOIN
                hc_mainDep md ON pl.main_dep = md.id
                    LEFT JOIN
                hc_codigos_Departamento hd ON pl.hc_dep = hd.id
                    LEFT JOIN
                hc_codigos_Puesto hp ON pl.hc_puesto = hp.id
                    LEFT JOIN
                hc_codigos_Areas ha ON hd.area = ha.id
                    LEFT JOIN
                hc_codigos_UnidadDeNegocio hu ON ha.unidadDeNegocio = hu.id
                    LEFT JOIN
                PCRCs pr ON pl.departamento = pr.id
                    LEFT JOIN
                PCRCs_puestos pp ON pl.puesto = pp.id
                    LEFT JOIN
                PDVs pdv ON pl.oficina = pdv.id
                    LEFT JOIN
                db_municipios cd ON pl.ciudad = cd.id
                    LEFT JOIN
                dep_asesores das ON pl.id = das.vacante
                    AND das.Fecha = CURDATE()
                    LEFT JOIN
                (SELECT 
                    MAX(Fecha_out) as f_Out,
                        IF(MAX(Fecha_in) < MAX(Fecha_out), NULL, MAX(Fecha_in)) as f_In,
                        vacante
                FROM
                    asesores_movimiento_vacantes
                GROUP BY vacante) dts ON pl.id = dts.vacante
                LEFT JOIN
                (SELECT 
                  a.vacante, a.asesor_out as asesor
                FROM
                  asesores_movimiento_vacantes a
                    RIGHT JOIN
                  (SELECT 
                    vacante, MAX(Fecha_out) AS fo
                  FROM
                    asesores_movimiento_vacantes
                  GROUP BY vacante) b ON b.vacante = a.vacante
                    AND a.Fecha_out = b.fo) lib ON pl.id = lib.vacante
            HAVING 
              pl.Status < 3
                AND UDN IS NOT NULL
            ORDER BY UDN, Area, Oficina, Departamento, Puesto, copcPuesto, pl.Status, pl.Activo, cubiertaPor, dtLiberada
            ");
      
      $this->db->select('*')
              ->from('hc');

      if( isset( $udn ) ){
        $this->db->where_in('UDN_id', explode('|', $udn) );
      }

      if( $q = $this->db->get( ) ){
                    
        okResponse( 'Info Obtenida', 'data', $q->result_array(), $this );
        
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }
      
    });
  }


}
