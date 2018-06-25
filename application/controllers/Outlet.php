<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Outlet extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function slots_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $query = "SELECT 
                    Fecha, Hora, COUNT(*) citas
                FROM
                    comeycom_WFM.ovv_citas
                GROUP BY Fecha , Hora";
        
      if( $q = $this->db->query( $query ) ){
          
          $result = array();
          foreach($q->result_array() as $index => $info){
              $result[$info['Fecha']][$info['Hora']] = $info['citas'];
          }
          
        okResponse( 'Información Obtenida', 'data', $result, $this );

      }else{

        errResponse('Error al obtener slots en tiempo real', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
    public function cita_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        
        $data['Nombre'] = ucwords(strtolower(trim($data['Nombre'])));

        $this->db->set($data)
            ->set('creator', $_GET['usid']);
        
      if( $q = $this->db->insert( 'ovv_citas' ) ){
          
        okResponse( 'Información Guardada', 'data', $this->db->insert_id(), $this );

      }else{

        errResponse('Error guardar cita', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  } 
    
    public function citaUpdate_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $dt = $this->put();
        $data = $dt['params'];
        $folio = $dt['folio'];
        
        $data['Nombre'] = ucwords(strtolower(trim($data['Nombre'])));

        $this->db->set($data)
            ->set('creator', $_GET['usid'])
            ->where('id', $folio);
        
      if( $q = $this->db->update( 'ovv_citas' ) ){
          
        okResponse( 'Información Guardada', 'data', $folio, $this );

      }else{

        errResponse('Error guardar cita', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  } 
    
    public function citaDelete_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $folio = $this->uri->segment(3);

        $this->db->where('id', $folio);
        
      if( $q = $this->db->delete( 'ovv_citas' ) ){
          
        okResponse( 'Información Borrada', 'data', $folio, $this );

      }else{

        errResponse('Error guardar cita', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  } 
    
    public function download_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $query = "SELECT 
                    *, NOMBREASESOR(creator, 2) AS creador
                FROM
                    ovv_citas
                ORDER BY Fecha , Hora";
        
      if( $q = $this->db->query( $query ) ){
          
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
    public function db_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $this->db->from('ovv_db a')->order_by('VentaMxn', 'DESC');
        
        $monitor = $this->uri->segment(3);
        
        if(isset($monitor) && $monitor == 1){
            $this->db->select('id, a.status_changer, Contacto, Status, folio, NOMBREASESOR(a.status_changer,1) as ActualizadoPor, comments', FALSE);
        }else{
            $this->db->select('*, NOMBREASESOR(a.status_changer,1) as ActualizadoPor', FALSE);
        }
    
        $this->db->where('Contacto <',2);
        
      if( $q = $this->db->get() ){
          
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
    public function lvSt_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $this->db->from('ovv_db a');
        
        $loc = $this->uri->segment(3);
        
        $this->db->select('live_status')->where('Localizador',$loc);

      if( $q = $this->db->get() ){
          
        okResponse( 'Información Obtenida', 'data', $q->row_array(), $this );

      }else{

        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  } 
    
  public function statusChg_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        
        $this->db->set( $data['field'], $data['val'] )
                ->set( 'status_changer', $_GET['usid'])
                ->where('id', $data['id']);
                
        
        if( $data['field'] == 'Status' ){
            $this->db->set( 'comments', $data['comments']);     
            if( $data['val'] == 1 ){
                $this->db->set( 'folio', $data['folio']);
            }
        }
        
     
        if( $this->db->update('ovv_db') ){
          
            okResponse( 'Información Guardada', 'data', true, $this );

          }else{

            errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

          }

      return true;

    });

    jsonPrint( $result );

  }
    
    public function dashPorHora_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS lyTable");
            $this->db->query("CREATE TEMPORARY TABLE lyTable SELECT 
                a.*, b.*
            FROM
                (SELECT 
                    a.Fecha, Hora_group, Hora_time
                FROM
                    Fechas a
                JOIN HoraGroup_Table15 b 
                WHERE
                    a.Fecha BETWEEN '20180510' AND '20180513') a
                    LEFT JOIN
                (SELECT 
                    ADDDATE(Fecha, 364) AS FechaOK,
                        HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HGOK,
                        SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto
                FROM
                    t_Locs a
                LEFT JOIN chanGroups b ON a.chanId = b.id
                WHERE
                    Fecha BETWEEN '20170511' AND '20170514'
                        AND gpoCanalKpi = 'Outlet'
                        AND pais = 'MX'
                GROUP BY marca , FechaOK , HGOK) b ON a.Fecha = b.FechaOK
                    AND a.Hora_group = b.HGOK");

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tdTable");
            $this->db->query("CREATE TEMPORARY TABLE tdTable SELECT 
                a.Fecha,
                HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HG,
                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                SUM(IF(servicio = 'Hotel',
                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                    0)) AS Hotel
            FROM
                t_hoteles_test a
                    LEFT JOIN
                chanGroups b ON a.chanId = b.id
                    LEFT JOIN
                itemTypes c ON a.itemType = c.type
                    AND a.categoryId = c.category
            WHERE
                a.Fecha BETWEEN '20180510' AND '20180513'
                    AND pais = 'MX'
                    AND gpoCanalKpi = 'Outlet'
            GROUP BY a.Fecha , HG");

            $query = "SELECT 
                a.Fecha,
                Hora_group, CAST(CONCAT(a.Fecha,' ',Hora_time) as DATETIME) as Hora_time,
                COALESCE(a.Monto,0) AS MontoLY,
                COALESCE(b.Monto,0) AS MontoTY,
                COALESCE(Hotel,0) AS Monto_Hotel,
                presupuesto AS meta,
                p_hotel AS meta_hotel,
                meta_bi,
                meta_bi_hoteles
            FROM
                lyTable a
                    LEFT JOIN
                tdTable b ON a.Fecha = b.Fecha
                    AND a.Hora_group = b.HG LEFT JOIN ovv_metas c ON a.Fecha=c.Fecha ORDER BY Fecha, Hora_group";




            if( $q = $this->db->query($query) ){
                
                $l = $this->db->query("SELECT MAX(Last_Update) as LU FROM t_hoteles_test WHERE Fecha BETWEEN '20180510' AND '20180514'");

                $result = $q->result_array();

                $acum = array('ly' => 0, 'ty' => 0, 'hotel' => 0, 'meta' => 0, 'meta_hotel' => 0, 'meta_bi' => 0, 'meta_bi_hoteles' => 0);
                $tmp = array();
                $data = array();
                $acumulado = array();
                

                foreach($result as $index => $item){

                    if( !isset( $tmp[ $item['Fecha']] ) ){
                        
                        $tmp[ $item['Fecha']] = array('ly' => 0, 'ty' => 0, 'hotel' => 0, 'meta' => floatval($item['meta']), 'meta_hotel' => floatval($item['meta_hotel']), 'meta_bi' => floatval($item['meta_bi']), 'meta_bi_hoteles' => floatval($item['meta_bi_hoteles']));
                        
                        $acum['meta'] += $item['meta'];
                        $acum['meta_hotel'] += $item['meta_hotel'];
                        $acum['meta_bi'] += $item['meta_bi'];
                        $acum['meta_bi_hoteles'] += $item['meta_bi_hoteles'];
                        
                    }
                    
                    $tmpArr = array(
                        'ly' => $tmp[ $item['Fecha']]['ly'] + $item['MontoLY'],
                        'ty' => $tmp[ $item['Fecha']]['ty'] + $item['MontoTY'],
                        'hotel' => $tmp[ $item['Fecha']]['hotel'] + $item['Monto_Hotel'],
                        'meta' => floatval($item['meta']),
                        'meta_hotel' => floatval($item['meta_hotel']),
                        'meta_bi' => floatval($item['meta_bi']),
                        'meta_bi_hoteles' => floatval($item['meta_bi_hoteles']),
                        'time' => $item['Hora_time']
                    );
                    
                    $tmpAcum = array(
                        'ly' => $acum['ly'] + $item['MontoLY'],
                        'ty' => $acum['ty'] + $item['MontoTY'],
                        'hotel' => $acum['hotel'] + $item['Monto_Hotel'],
                        'meta' => floatval($acum['meta']),
                        'meta_hotel' => floatval($acum['meta_hotel']),
                        'meta_bi' => floatval($acum['meta_bi']),
                        'meta_bi_hoteles' => floatval($acum['meta_bi_hoteles']),
                        'time' => $item['Hora_time']
                    );
                    
                    if( isset($data[ $item['Fecha'] ]) ){
                        array_push($data[ $item['Fecha']], $tmpArr);
                    }else{
                        $data[ $item['Fecha'] ] = array($tmpArr);
                    }
                    
                    array_push($acumulado, $tmpAcum);

                    $tmp[ $item['Fecha'] ]['ly'] += $item['MontoLY'];
                    $tmp[ $item['Fecha'] ]['ty'] += $item['MontoTY'];
                    $tmp[ $item['Fecha'] ]['hotel'] += $item['Monto_Hotel'];
                    $acum['ly'] += $item['MontoLY'];
                    $acum['ty'] += $item['MontoTY'];
                    $acum['hotel'] += $item['Monto_Hotel'];
                    
                    
                }
                
                $data['Todo']=$acumulado;
                
                $canal = $this->xCanal();

                okResponse( 'Información Guardada', 'data',  array('all' => $data, 'canal' => $canal), $this, 'lu', $l->row_array() );

              }else{

                errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

              }

          return true;
        });
                                
    }
    
    private function xCanal(){
        
        
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS lyTable");
            $this->db->query("CREATE TEMPORARY TABLE lyTable SELECT 
                a.*, b.*
            FROM
                (SELECT 
                    a.Fecha, Hora_group, Hora_time, canal
                FROM
                    Fechas a
                JOIN HoraGroup_Table15 b JOIN chanGroups c
                WHERE
                    a.Fecha BETWEEN '20180510' AND '20180513' AND gpoCanalKPI='Outlet') a
                    LEFT JOIN
                (SELECT 
                    ADDDATE(Fecha, 364) AS FechaOK,
                        HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HGOK,
                        SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto, canal as canalOK
                FROM
                    t_Locs a
                LEFT JOIN chanGroups b ON a.chanId = b.id
                WHERE
                    Fecha BETWEEN '20170511' AND '20170514'
                        AND gpoCanalKpi = 'Outlet'
                        AND pais = 'MX'
                GROUP BY marca , FechaOK , HGOK, canal) b ON a.Fecha = b.FechaOK AND a.canal=b.canalOK
                    AND a.Hora_group = b.HGOK");
            $this->db->query("ALTER TABLE lyTable ADD PRIMARY KEY (Fecha, Hora_group, canal(25))");
            
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tdTable");
            $this->db->query("CREATE TEMPORARY TABLE tdTable SELECT 
                a.Fecha,
                HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HG,
                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                SUM(IF(servicio = 'Hotel',
                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                    0)) AS Hotel, canal
            FROM
                t_hoteles_test a
                    LEFT JOIN
                chanGroups b ON a.chanId = b.id
                    LEFT JOIN
                itemTypes c ON a.itemType = c.type
                    AND a.categoryId = c.category
            WHERE
                a.Fecha BETWEEN '20180510' AND '20180513'
                    AND pais = 'MX'
                    AND gpoCanalKpi = 'Outlet'
            GROUP BY a.Fecha , HG, canal");
            $this->db->query("ALTER TABLE tdTable ADD PRIMARY KEY (Fecha, HG, canal(25))");

            $query = "SELECT 
                a.Fecha,
                IF(a.canal LIKE '%PriceTravel%', 'Outlet PriceTravel', a.canal) as canal,
                Hora_group, CAST(CONCAT(a.Fecha,' ',Hora_time) as DATETIME) as Hora_time,
                COALESCE(a.Monto,0) AS MontoLY,
                COALESCE(b.Monto,0) AS MontoTY,
                COALESCE(Hotel,0) AS Monto_Hotel,
                presupuesto AS meta,
                p_hotel AS meta_hotel,
                meta_bi,
                meta_bi_hoteles
            FROM
                lyTable a
                    LEFT JOIN
                tdTable b ON a.Fecha = b.Fecha
                    AND a.Hora_group = b.HG AND a.canal=b.canal LEFT JOIN ovv_metas c ON a.Fecha=c.Fecha WHERE a.canal LIKE '%Outlet%' ORDER BY Fecha, Hora_group";




            if( $q = $this->db->query($query) ){
                

                $result = $q->result_array();
                $arr = array();
                
                foreach($result as $index => $item ){
                    if( isset($arr[$item['canal']]) ){
                        array_push($arr[$item['canal']], $item);
                    }else{
                        $arr[$item['canal']] = array($item);
                    }
                }
                
                foreach($arr as $canal => $info){
                    $data[$canal] = $this->buildPH($info);
                }
                

                return $data;

              }else{

                errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

              }
      
    }
    
    public function v2018dashPorHora_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS lyTable");
            $this->db->query("CREATE TEMPORARY TABLE lyTable SELECT 
                a.*, b.*
            FROM
                (SELECT 
                    a.Fecha, Hora_group, Hora_time
                FROM
                    Fechas a
                JOIN HoraGroup_Table15 b 
                WHERE
                    a.Fecha BETWEEN '20180514' AND '20180520') a
                    LEFT JOIN
                (SELECT 
                    ADDDATE(Fecha, 364) AS FechaOK,
                        HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HGOK,
                        SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto
                FROM
                    t_Locs a
                LEFT JOIN chanGroups b ON a.chanId = b.id
                WHERE
                    Fecha BETWEEN '20170515' AND '20170521'
                        AND pais = 'MX'
                        AND marca = 'Marcas Propias'
                GROUP BY marca , FechaOK , HGOK) b ON a.Fecha = b.FechaOK
                    AND a.Hora_group = b.HGOK");

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tdTable");
            $this->db->query("CREATE TEMPORARY TABLE tdTable SELECT 
                a.Fecha,
                HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HG,
                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                SUM(IF(servicio = 'Hotel',
                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                    0)) AS Hotel
            FROM
                t_hoteles_test a
                    LEFT JOIN
                chanGroups b ON a.chanId = b.id
                    LEFT JOIN
                itemTypes c ON a.itemType = c.type
                    AND a.categoryId = c.category
            WHERE
                a.Fecha BETWEEN '20180514' AND '20180520'
                    AND pais = 'MX'
                    AND marca = 'Marcas Propias'
            GROUP BY a.Fecha , HG");

            $query = "SELECT 
                a.Fecha,
                Hora_group, CAST(CONCAT(a.Fecha,' ',Hora_time) as DATETIME) as Hora_time,
                COALESCE(a.Monto,0) AS MontoLY,
                COALESCE(b.Monto,0) AS MontoTY,
                COALESCE(Hotel,0) AS Monto_Hotel,
                presupuesto AS meta,
                p_hotel AS meta_hotel,
                meta_bi,
                meta_bi_hoteles
            FROM
                lyTable a
                    LEFT JOIN
                tdTable b ON a.Fecha = b.Fecha
                    AND a.Hora_group = b.HG LEFT JOIN ovv_metas c ON a.Fecha=c.Fecha ORDER BY Fecha, Hora_group";




            if( $q = $this->db->query($query) ){
                
                $l = $this->db->query("SELECT MAX(Last_Update) as LU FROM t_hoteles_test WHERE Fecha BETWEEN '20180510' AND '20180514'");

                $result = $q->result_array();

                $acum = array('ly' => 0, 'ty' => 0, 'hotel' => 0, 'meta' => 0, 'meta_hotel' => 0, 'meta_bi' => 0, 'meta_bi_hoteles' => 0);
                $tmp = array();
                $data = array();
                $acumulado = array();
                

                foreach($result as $index => $item){

                    if( !isset( $tmp[ $item['Fecha']] ) ){
                        
                        $tmp[ $item['Fecha']] = array('ly' => 0, 'ty' => 0, 'hotel' => 0, 'meta' => floatval($item['meta']), 'meta_hotel' => floatval($item['meta_hotel']), 'meta_bi' => floatval($item['meta_bi']), 'meta_bi_hoteles' => floatval($item['meta_bi_hoteles']));
                        
                        $acum['meta'] += $item['meta'];
                        $acum['meta_hotel'] += $item['meta_hotel'];
                        $acum['meta_bi'] += $item['meta_bi'];
                        $acum['meta_bi_hoteles'] += $item['meta_bi_hoteles'];
                        
                    }
                    
                    $tmpArr = array(
                        'ly' => $tmp[ $item['Fecha']]['ly'] + $item['MontoLY'],
                        'ty' => $tmp[ $item['Fecha']]['ty'] + $item['MontoTY'],
                        'hotel' => $tmp[ $item['Fecha']]['hotel'] + $item['Monto_Hotel'],
                        'meta' => floatval($item['meta']),
                        'meta_hotel' => floatval($item['meta_hotel']),
                        'meta_bi' => floatval($item['meta_bi']),
                        'meta_bi_hoteles' => floatval($item['meta_bi_hoteles']),
                        'time' => $item['Hora_time']
                    );
                    
                    $tmpAcum = array(
                        'ly' => $acum['ly'] + $item['MontoLY'],
                        'ty' => $acum['ty'] + $item['MontoTY'],
                        'hotel' => $acum['hotel'] + $item['Monto_Hotel'],
                        'meta' => floatval($acum['meta']),
                        'meta_hotel' => floatval($acum['meta_hotel']),
                        'meta_bi' => floatval($acum['meta_bi']),
                        'meta_bi_hoteles' => floatval($acum['meta_bi_hoteles']),
                        'time' => $item['Hora_time']
                    );
                    
                    if( isset($data[ $item['Fecha'] ]) ){
                        array_push($data[ $item['Fecha']], $tmpArr);
                    }else{
                        $data[ $item['Fecha'] ] = array($tmpArr);
                    }
                    
                    array_push($acumulado, $tmpAcum);

                    $tmp[ $item['Fecha'] ]['ly'] += $item['MontoLY'];
                    $tmp[ $item['Fecha'] ]['ty'] += $item['MontoTY'];
                    $tmp[ $item['Fecha'] ]['hotel'] += $item['Monto_Hotel'];
                    $acum['ly'] += $item['MontoLY'];
                    $acum['ty'] += $item['MontoTY'];
                    $acum['hotel'] += $item['Monto_Hotel'];
                    
                    
                }
                
                $data['Todo']=$acumulado;
                
                $canal = $this->v2018xCanal();

                okResponse( 'Información Guardada', 'data',  array('all' => $data, 'canal' => $canal), $this, 'lu', $l->row_array() );

              }else{

                errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

              }

          return true;
        });
                                
    }
    
    private function v2018xCanal(){
        
        
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS lyTable");
            $this->db->query("CREATE TEMPORARY TABLE lyTable SELECT 
                a.*, b.*
            FROM
                (SELECT 
                    a.Fecha, Hora_group, Hora_time, gpoTipoRsva as canal
                FROM
                    Fechas a
                JOIN HoraGroup_Table15 b JOIN (SELECT DISTINCT gpoTipoRsva FROM config_tipoRsva) c
                WHERE
                    a.Fecha BETWEEN '20180514' AND '20180520') a
                    LEFT JOIN
                (SELECT 
                    ADDDATE(a.Fecha, 364) AS FechaOK,
                        HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HGOK,
                        SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto, gpoTipoRsva as canalOK
                FROM
                    t_Locs a
                LEFT JOIN chanGroups b ON a.chanId = b.id LEFT JOIN dep_asesores c ON a.asesor=c.asesor AND a.Fecha=c.Fecha LEFT JOIN config_tipoRsva d ON 
                IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo
                                        AND IF(c.dep IS NULL,
                                        IF(a.asesor = - 1, - 1, 0),
                                        IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                            0,
                                            c.dep)) = d.dep
                WHERE
                    a.Fecha BETWEEN '20170515' AND '20170521'
                        AND marca = 'Marcas Propias'
                        AND pais = 'MX'
                GROUP BY marca , FechaOK , HGOK, gpoTipoRsva) b ON a.Fecha = b.FechaOK AND a.canal=b.canalOK
                    AND a.Hora_group = b.HGOK");
            $this->db->query("ALTER TABLE lyTable ADD PRIMARY KEY (Fecha, Hora_group, canal(25))");
        
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS Locs");
            $this->db->query("CREATE TEMPORARY TABLE Locs SELECT 
                Localizador, a.asesor, gpoTipoRsva
            FROM
                d_Locs a
                    LEFT JOIN
                dep_asesores b ON a.asesor = b.asesor
                    AND a.Fecha = b.Fecha
                    LEFT JOIN
                config_tipoRsva c ON IF(a.tipo IS NULL OR a.tipo = '',
                    0,
                    a.tipo) = c.tipo
                    AND IF(b.dep IS NULL,
                    IF(a.asesor = - 1, - 1, 0),
                    IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                        0,
                        b.dep)) = c.dep
            WHERE
                a.Fecha BETWEEN '20180514' AND '20180520'
            GROUP BY Localizador");
            $this->db->query("ALTER TABLE Locs ADD PRIMARY KEY (Localizador)");
            
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tdTable");
            $this->db->query("CREATE TEMPORARY TABLE tdTable SELECT 
                a.Fecha,
                HOUR(Hora) + IF(MINUTE(Hora) >= 15, IF(MINUTE(Hora) >= 30, IF(MINUTE(Hora) >= 45, .75, .50), .25), 0) AS HG,
                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                SUM(IF(servicio = 'Hotel',
                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                    0)) AS Hotel, gpoTipoRsva as canal
            FROM
                t_hoteles_test a
                    LEFT JOIN
                chanGroups b ON a.chanId = b.id
                    LEFT JOIN
                itemTypes c ON a.itemType = c.type
                    AND a.categoryId = c.category LEFT JOIN Locs d ON a.Localizador=d.Localizador 
            WHERE
                a.Fecha BETWEEN '20180514' AND '20180520'
                    AND pais = 'MX'
                    AND marca = 'Marcas Propias'
            GROUP BY a.Fecha , HG, gpoTipoRsva");
            $this->db->query("ALTER TABLE tdTable ADD PRIMARY KEY (Fecha, HG, canal(25))");

            $query = "SELECT 
                a.Fecha,
                a.canal,
                Hora_group, CAST(CONCAT(a.Fecha,' ',Hora_time) as DATETIME) as Hora_time,
                COALESCE(a.Monto,0) AS MontoLY,
                COALESCE(b.Monto,0) AS MontoTY,
                COALESCE(Hotel,0) AS Monto_Hotel,
                presupuesto AS meta,
                p_hotel AS meta_hotel,
                meta_bi,
                meta_bi_hoteles
            FROM
                lyTable a
                    LEFT JOIN
                tdTable b ON a.Fecha = b.Fecha
                    AND a.Hora_group = b.HG AND a.canal=b.canal LEFT JOIN ovv_metas c ON a.Fecha=c.Fecha ORDER BY Fecha, Hora_group";




            if( $q = $this->db->query($query) ){
                
                $data = array();
                $result = $q->result_array();
                $arr = array();
                
                foreach($result as $index => $item ){
                    if( isset($arr[$item['canal']]) ){
                        array_push($arr[$item['canal']], $item);
                    }else{
                        $arr[$item['canal']] = array($item);
                    }
                }
                
                foreach($arr as $canal => $info){
                    $data[$canal] = $this->buildPH($info);
                }
                

                return $data;

              }else{

                errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

              }
      
    }
    
    private function buildPH($arr){
        $acum = array('ly' => 0, 'ty' => 0, 'hotel' => 0);
        $tmp = array();
        $data = array();
        $acumulado = array();


        foreach($arr as $index => $item){

            if( !isset( $tmp[ $item['Fecha']] ) ){

                $tmp[ $item['Fecha']] = array('ly' => 0, 'ty' => 0, 'hotel' => 0);

            }

            $tmpArr = array(
                'ly' => $tmp[ $item['Fecha']]['ly'] + $item['MontoLY'],
                'ty' => $tmp[ $item['Fecha']]['ty'] + $item['MontoTY'],
                'hotel' => $tmp[ $item['Fecha']]['hotel'] + $item['Monto_Hotel'],
                'time' => $item['Hora_time']
            );

            $tmpAcum = array(
                'ly' => $acum['ly'] + $item['MontoLY'],
                'ty' => $acum['ty'] + $item['MontoTY'],
                'hotel' => $acum['hotel'] + $item['Monto_Hotel'],
                'time' => $item['Hora_time']
            );

            if( isset($data[ $item['Fecha'] ]) ){
                array_push($data[ $item['Fecha']], $tmpArr);
            }else{
                $data[ $item['Fecha'] ] = array($tmpArr);
            }

            array_push($acumulado, $tmpAcum);

            $tmp[ $item['Fecha'] ]['ly'] += $item['MontoLY'];
            $tmp[ $item['Fecha'] ]['ty'] += $item['MontoTY'];
            $tmp[ $item['Fecha'] ]['hotel'] += $item['Monto_Hotel'];
            $acum['ly'] += $item['MontoLY'];
            $acum['ty'] += $item['MontoTY'];
            $acum['hotel'] += $item['Monto_Hotel'];


        }

        $data['Todo']=$acumulado;
        
        return $data;
    }


 
}
