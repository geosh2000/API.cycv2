<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Procesos extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function grafAsesores_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $id = $this->uri->segment(3);
        
        $asesor = $this->uri->segment(4);

        if( !$this->db->query("SET @inicio = CAST(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01') AS DATE)") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("SET @fin = ADDDATE(CURDATE(), - 1)") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        $this->db->select("Fecha, asesor, dep")
                  ->select("IF(dep IN (35,29,5),35,dep) as Skill", FALSE)
                  ->from("dep_asesores")
                  ->where("vacante IS NOT NULL", NULL, FALSE)
                  ->where("Fecha BETWEEN @inicio AND CURDATE()", NULL, FALSE);
        
        if( isset($asesor) ){
            $this->db->where('asesor',$asesor);
        }

        if( !$g_asesores = $this->db->get_compiled_select() ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$this->db->query("DROP TEMPORARY TABLE IF EXISTS g_asesores") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("CREATE TEMPORARY TABLE g_asesores $g_asesores") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("ALTER TABLE g_asesores  ADD PRIMARY KEY(Fecha, asesor)") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$this->db->query("DROP TEMPORARY TABLE IF EXISTS locBalance") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        $this->db->select("b.*,
                            Localizador,
                            SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Balance,
                            IF(SUM(VentaMXN) > 0
                                      AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                                      AND gpoTipoRsva = 'In',
                                  Localizador,
                                  NULL) AS NewLocIn,
                          	IF(SUM(VentaMXN) > 0
                                      AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                                      AND gpoTipoRsva != 'In',
                                  Localizador,
                                  NULL) AS NewLocElse,", FALSE)
                  ->from("t_Locs a")
                  ->join("g_asesores b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", "left")
                  // ->join("config_tipoRsva c", "IF(a.asesor!=-1 AND b.dep IN (35,5,29), b.dep, 0)=c.dep AND IF(ISNULL(a.tipo)=1 OR a.tipo='',0,a.tipo)=c.tipo", "left", FALSE)
                  ->join("config_tipoRsva c", "IF(a.asesor!=-1, b.dep, 0)=c.dep AND IF(ISNULL(a.tipo)=1 OR a.tipo='',0,a.tipo)=c.tipo", "left", FALSE)
                  ->where("a.Fecha BETWEEN @inicio AND CURDATE()", NULL, FALSE)
                  ->group_by( array("a.Fecha", "Localizador") )
                  ->having("asesor IS NOT NULL", NULL, FALSE);

        if( !$locBalance = $this->db->get_compiled_select() ){
          errResponse('Error en la base de datos locs', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("CREATE TEMPORARY TABLE locBalance $locBalance") ){
          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $locBalance);
        }
        if( !$this->db->query("ALTER TABLE locBalance  ADD PRIMARY KEY(Fecha, Localizador)") ){
          errResponse('Error en la base de datos 69', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$this->db->query("DROP TEMPORARY TABLE IF EXISTS callsAsesor") ){
          errResponse('Error en la base de datos 73', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        $this->db->select("a.*, b.dep, b.Skill as aSkill, c.Skill, c.direction", FALSE)
                ->from("t_Answered_Calls a")
                ->join("g_asesores b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", "left")
                ->join("Cola_Skill c", "a.Cola = c.Cola", "left")
                ->where("a.Fecha BETWEEN @inicio AND @fin", NULL, FALSE)
                ->having("!(Desconexion = 'Transferida' AND TIME_TO_SEC(Duracion_Real) <= 120)", NULL, FALSE)
                ->having("aSkill = c.Skill", NULL, FALSE)
                ->having("c.direction", 1, FALSE);
        
        if( isset($asesor) ){
            $this->db->where('a.asesor',$asesor);
        }
        
        if( !$callAsesorINSIDE = $this->db->get_compiled_select() ){
          errResponse('Error en la base de datos 82', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        $this->db->select("Fecha, asesor, COUNT(*) AS llamadas", FALSE)
                  ->from("($callAsesorINSIDE) a ", FALSE)
                  ->group_by( array('a.Fecha', 'a.asesor') );

        if( !$callAsesor = $this->db->get_compiled_select() ){
          errResponse('Error en la base de datos calls', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("CREATE TEMPORARY TABLE callsAsesor $callAsesor") ){
          errResponse('Error en la base de datos 93', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("ALTER TABLE callsAsesor ADD PRIMARY KEY(Fecha, asesor(10))") ){
          errResponse('Error en la base de datos 96', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$this->db->query("DROP TEMPORARY TABLE IF EXISTS ausAsesor") ){
          errResponse('Error en la base de datos 100', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        $this->db->select("a.Fecha, a.asesor, IF(js != je AND ausent_id IS NULL, 1, 0) as Ausentismo", FALSE)
                  ->from("asesores_programacion a")
                  ->join("g_asesores b", "a.Fecha = b.Fecha AND a.asesor = b.asesor", "left")
                  ->join("Ausentismos c", "a.Fecha BETWEEN Inicio AND Fin AND a.asesor=c.asesor", "left")
                  ->where("a.Fecha BETWEEN @inicio AND @fin AND b.asesor IS NOT NULL", NULL, FALSE);
        
        if( isset($asesor) ){
            $this->db->where('a.asesor',$asesor);
        }

        if( !$ausAsesor = $this->db->get_compiled_select() ){
          errResponse('Error en la base de datos 109', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("CREATE TEMPORARY TABLE ausAsesor $ausAsesor") ){
          errResponse('Error en la base de datos 112', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if( !$this->db->query("ALTER TABLE ausAsesor  ADD PRIMARY KEY(Fecha, asesor)") ){
          errResponse('Error en la base de datos 115', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$this->db->query("DROP TEMPORARY TABLE IF EXISTS locsAsesor") ){
          errResponse('Error en la base de datos 119', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        $this->db->select("b.Fecha,
                            b.asesor,
                            COUNT(DISTINCT NewLocIn) AS LocsIn,
                            COUNT(DISTINCT NewLocElse) AS LocsElse,
                            SUM(IF(!ISNULL(NewLocIn), VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoIn,
                            SUM(IF(!ISNULL(NewLocElse), VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoElse", FALSE)
                  ->from("t_Locs a")
                  ->join("locBalance b", "a.Fecha = b.Fecha AND a.Localizador = b.Localizador", "left")
                  ->where("a.Fecha BETWEEN @inicio AND @fin AND b.asesor IS NOT NULL ", NULL, FALSE)
                  ->group_by( array("b.Fecha", "b.asesor") );
        
        if( isset($asesor) ){
            $this->db->where('a.asesor',$asesor);
        }

        $locsAsesor = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE locsAsesor $locsAsesor");
        $this->db->query("ALTER TABLE locsAsesor  ADD PRIMARY KEY(Fecha, asesor)");
        
        $this->db->where("Fecha BETWEEN ", "@inicio AND @fin", FALSE);
        if( isset($asesor) ){
            $this->db->where('asesor',$asesor);
        }

        $this->db->delete("graf_ventaDiariaAsesores");

        if( $this->db->query("INSERT INTO graf_ventaDiariaAsesores (SELECT
  a.Fecha, a.asesor, IF(llamadas IS NULL,0,llamadas) as llamadas, LocsIn, LocsElse, MontoIn, MontoElse, IF(Ausentismo IS NULL,0,Ausentismo) as Ausentismo, NULL
                                                                  FROM
                                                                    g_asesores a
                                                                  	  LEFT JOIN
                                                                    locsAsesor b ON a.asesor = b.asesor
                                                                  	  AND a.Fecha = b.Fecha
                                                                  	  LEFT JOIN
                                                                    callsAsesor c ON a.Fecha = c.Fecha
                                                                  	  AND a.asesor = c.asesor
                                                                  	  LEFT JOIN
                                                                    ausAsesor d ON a.Fecha = d.Fecha
                                                                  	  AND a.asesor = d.asesor WHERE a.Fecha<CURDATE())") ){

          if( !isset($asesor) ){
            if( $this->db->query("UPDATE config_reportUpdates SET lastRun = NOW() WHERE id=$id") ){
                okResponse( "Reporte corrió correctamente", 'data', true, $this );
              }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
              }
          }else{
              okResponse( "Reporte corrió correctamente", 'data', true, $this );
          }

        }else{

          errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }

      return true;

    });

    $this->response( $result );

  }

  public function grafAsesoresDaily_get(){

      $this->db->query("SELECT * FROM graf_ventaDiariaAsesores");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS g_asesores");
      $this->db->query("CREATE TEMPORARY TABLE g_asesores SELECT
                  Fecha, asesor, dep, IF(dep IN (35,29, 5),35,dep) as Skill
              FROM
                  dep_asesores
              WHERE
                  vacante IS NOT NULL
                      AND Fecha = CURDATE()");

      $this->db->query("ALTER TABLE g_asesores  ADD PRIMARY KEY(Fecha, asesor)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS locBalance");
      $this->db->query("CREATE TEMPORARY TABLE locBalance SELECT
                  b.*,
                  Localizador,
                  SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Balance,
                  IF(SUM(VentaMXN) > 0
                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                            AND gpoTipoRsva = 'In',
                        Localizador,
                        NULL) AS NewLocIn,
                	IF(SUM(VentaMXN) > 0
                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                            AND gpoTipoRsva != 'In',
                        Localizador,
                        NULL) AS NewLocElse
              FROM
                  d_Locs a
                      LEFT JOIN
                  g_asesores b ON a.asesor = b.asesor
                      AND a.Fecha = b.Fecha
                      LEFT JOIN
                  config_tipoRsva c ON IF(a.asesor != - 1,
                      b.dep,
                      0) = c.dep
                      AND IF(ISNULL(a.tipo)=1 OR a.tipo='',0,a.tipo) = c.tipo
              WHERE
                  a.Fecha = CURDATE()
              GROUP BY a.Fecha , Localizador
              HAVING asesor IS NOT NULL");

      $this->db->query("ALTER TABLE locBalance  ADD PRIMARY KEY(Fecha, Localizador)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS tdCalls");
      $this->db->query("CREATE TEMPORARY TABLE tdCalls SELECT
                  CAST(tstStart AS DATE) AS Fecha,
                GETIDASESOR(descr_agente, 2) AS asesor,
                Skill, direction
              FROM
                  ccexporter.callsDetails a
                      LEFT JOIN
                  ccexporter.agentDetails b ON a.agent = b.nome_agente
                  LEFT JOIN
                      Cola_Skill c ON a.queue=c.queue
              WHERE
                  callLen IS NOT NULL AND
                  tstStart >= CAST(CONCAT(CURDATE(),' 00:00:00') as DATE) HAVING direction=1");

      $this->db->query("ALTER TABLE tdCalls ADD INDEX `asesor` (Fecha ASC, asesor, Skill)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsAsesor");
      $this->db->query("CREATE TEMPORARY TABLE callsAsesor SELECT
                  a.Fecha, a.asesor, COUNT(*) AS llamadas
              FROM
                  tdCalls a
                      LEFT JOIN
                  g_asesores b ON a.Fecha = b.Fecha
                      AND a.asesor = b.asesor
              WHERE
                  b.Skill = a.Skill
              GROUP BY a.Fecha , a.asesor");

      $this->db->query("ALTER TABLE callsAsesor ADD PRIMARY KEY(Fecha, asesor)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS ausAsesor");
      $this->db->query("CREATE TEMPORARY TABLE ausAsesor SELECT
                  a.Fecha, a.asesor, IF(js != je AND ausent_id IS NULL, 1, 0) as Ausentismo
              FROM
                  asesores_programacion a
                      LEFT JOIN
                  g_asesores b ON a.Fecha = b.Fecha
                      AND a.asesor = b.asesor
                      LEFT JOIN
                      Ausentismos c ON a.Fecha BETWEEN Inicio AND Fin AND a.asesor=c.asesor
              WHERE
                  a.Fecha = CURDATE()
                      AND b.asesor IS NOT NULL");

      $this->db->query("ALTER TABLE ausAsesor  ADD PRIMARY KEY(Fecha, asesor)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsAsesor");
      $this->db->query("CREATE TEMPORARY TABLE locsAsesor SELECT
                  b.Fecha,
                  b.asesor,
                  COUNT(DISTINCT NewLocIn) AS LocsIn,
                  COUNT(DISTINCT NewLocElse) AS LocsElse,
                  SUM(IF(!ISNULL(NewLocIn), VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoIn,
                  SUM(IF(!ISNULL(NewLocElse), VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoElse
              FROM
                  d_Locs a
                      LEFT JOIN
                  locBalance b ON a.Fecha = b.Fecha
                      AND a.Localizador = b.Localizador
              WHERE
                  a.Fecha = CURDATE()
                      AND b.asesor IS NOT NULL
              GROUP BY b.Fecha , b.asesor");

      $this->db->query("ALTER TABLE locsAsesor  ADD PRIMARY KEY(Fecha, asesor)");

      if( $this->db->query("INSERT INTO graf_ventaDiariaAsesores
              (SELECT * FROM (SELECT
                                  a.Fecha, a.asesor, IF(llamadas IS NULL,0,llamadas) as llamadas, LocsIn, LocsElse, MontoIn, MontoElse, IF(Ausentismo IS NULL,0,Ausentismo) as Ausentismo, NULL
                              FROM
                                  g_asesores a
                                      LEFT JOIN
                                  locsAsesor b ON a.asesor = b.asesor
                                      AND a.Fecha = b.Fecha
                                      LEFT JOIN
                                  callsAsesor c ON a.Fecha = c.Fecha
                                      AND a.asesor = c.asesor
                                      LEFT JOIN
                                  ausAsesor d ON a.Fecha = d.Fecha
                                      AND a.asesor = d.asesor) a)
              ON DUPLICATE KEY UPDATE llamadas=a.llamadas, rsvas=a.LocsIn, rsvas_else=a.LocsElse, monto=a.MontoIn, monto_else=a.MontoElse, ausentismo=a.Ausentismo") ){
            okResponse( "Reporte corrió correctamente!", 'data', true, $this );
          }else{
            $err = $this->db->error();

            errResponse('Error en la base de datos '.$err, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
  }

  public function xferCheck_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->query("SET @inicio = ADDDATE(CURDATE(),-2)");
      $this->db->query("SET @fin = CURDATE()");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS tmpXfered");
      $this->db->query("CREATE TEMPORARY TABLE tmpXfered SELECT 0 as id, '00:00:00' as Duracion");

      if( $result = $this->db->query("SELECT SUM(Xfered) as found
                                        FROM
                                          (SELECT
                                            xferCheck(ac_id) as xFered
                                          FROM
                                            t_Answered_Calls
                                          WHERE
                                            Fecha BETWEEN @inicio AND @fin AND Answered=1) a") ){
        $foundData = $result->row_array();
        $found = $foundData['found'];

        if( $this->db->query("UPDATE
                                  t_Answered_Calls a
                                LEFT JOIN
                                  tmpXfered b ON a.ac_id = b.id
                                SET
                                  a.Duracion_Real=b.Duracion, Desconexion='Transferida'
                                WHERE
                                  Fecha BETWEEN @inicio AND @fin
                                    AND Answered=1
                                    AND b.id IS NOT NULL") ){

            $this->db->query("UPDATE config_reportUpdates SET lastRun=NOW() WHERE id=2");
            okResponse("$found Xfers aplicados correctamente", 'data', true, $this);
          }else{

            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });
  }

  public function uplCalls_get(){

    $name = $this->uri->segment(3);

    $calls = $this->csvProcess("$name.csv");
      
    $ansOK  = 0;
    $ansERR = 0;

    foreach($calls['data'] as $index => $info){
      if( $info['Llamante'] != '*' ){

          
           $this->db->set($info)
                    ->set('asesor',$calls['escaped'][$index]['asesor'], FALSE);
          $asesorQ = "asesor = ".$calls['escaped'][$index]['asesor'].", ";

        $query = $this->db->get_compiled_insert("t_Answered_Calls")." ON DUPLICATE KEY UPDATE $asesorQ Duracion = '".$info['Duracion']."', Duracion_Real = '".$info['Duracion_Real']."', Desconexion = '".$info['Desconexion']."', Answered = ".$info['Answered'];

        if( $this->db->query($query) ){
          $resultado['ans'][$index] = array( 'status' => TRUE, 'msg' => '');
          $ansOK++;
        }else{
          $resultado['ans'][$index] = array( 'status' => FALSE, 'msg' => $this->db->error(), 'query' => $query, 'info' => $info );
          $ansERR++;
        }
        // $result[] = array($info, $ans['escaped'][$index]);
      }
    }


    $res = array(
                  'Calls'               => $ansOK + $ansERR,
                  'UPL_OK'              => $ansOK,
                  'UPL_ERR'             => $ansERR,
                  'data'                => $resultado
                );

    okResponse('Data procesada', 'data', $res, $this);



  }
    

  public function readCsv_get(){

      $name = $this->uri->segment(3);
      $ext  = $this->uri->segment(4);
      $dir  = $this->uri->segment(5);
      
      $route = $_SERVER['DOCUMENT_ROOT']."/img/$dir/"; 
      $fname = $name.".".$ext;
      
      if ( ( $gestor = fopen( $route.$fname, "r" ) ) !== FALSE ) {

          $fila = 1;
          $data = array();

          while( ( $datos = fgetcsv( $gestor, 10000, ',' ) ) ){
              
            $tmp = array();
            foreach( $datos as $key => $info ){

              //FIELD NAME ASSING
              if( $fila == 1 ){
                  if( $key == 'Id de transacción' ){
                    $keys[$key] = 'id';
                  }else{  
                    $keys[$key] = strtolower(str_replace("á", "a", str_replace("é", "e", str_replace("í", "i", str_replace("ó", "o", str_replace("ú", "u", str_replace("ñ", "n", $info)))))));
                  }
              }else{
                $tmp[$keys[$key]] = trim($info);
              }
                
                
                
            }
              

            if( $fila != 1 ) array_push($data, $tmp);

            $fila++;
          }

          fclose( $gestor );
          unlink($route.$fname);
    }else{
          errResponse('Error en la data', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'ERROR');
    }

    okResponse('Data procesada', 'data', $data, $this);

  }

  private function csvProcess( $fname ){

    $route = $_SERVER['DOCUMENT_ROOT']."/img/calls/";

    if ( ( $gestor = fopen( $route.$fname, "r" ) ) !== FALSE ) {

      $fila = 1;
      $total= 0;
      $i    = 0;

      while( ( $datos = fgetcsv( $gestor, 1000, ';' ) ) ){

        foreach( $datos as $key => $info ){
            

          // FIELD NAME ASSING
          if( $i == 0 ){
            $keys[] = $info;
          }else{
            $field = $keys[$key];

            switch($field){
              case 'Fecha':
                $curYear  = date('Y');
                $curMonth = date('n');
                $month    = substr( $info, 0, 2 );

                if( $month == '12' && $curMonth == 1 ){
                  $year = $curYear - 1;
                }else{
                  $year = $curYear;
                }

                $fecha = "$year/".substr( $info, 0, 5 );
                $data[$i-1]['Fecha']  = $fecha;
                $data[$i-1]['Hora']   = substr($info, 7,100);
                break;

              case 'Administradas por':
              case 'Agente':

                if( strpos( $info, '(' ) ){
                  $agent = "IF(GETIDASESOR('".trim( substr( $info, 0, strpos( $info, '(' ) ) )."',2) IS NULL, '".trim( substr( $info, 0, strpos( $info, '(' ) ) )."', GETIDASESOR('".trim( substr( $info, 0, strpos( $info, '(' ) ) )."',2))";
                }else{
                  $agent = "'".trim($info)."'";
                }

                $dataEscape[$i-1]['asesor'] = $agent;
                break;

              case 'Pos.':
                $data[$i-1]['Pos'] = trim($info);
                break;

              case 'Posicion':
                $data[$i-1]['Pos_salida'] = trim($info);
                break;

              case 'Duración':
                $data[$i-1]['Duracion'] = trim($info);
                $data[$i-1]['Duracion_Real'] = trim($info);
                break;

              case 'Código':
                $data[$i-1]['Codigo'] = trim($info);
                break;

              case 'Server':
                $data[$i-1]['Srv'] = trim($info);
                break;

              case 'Asterisk ID':
                $data[$i-1]['AsteriskID'] = trim($info);
                break;

              case 'Eventos de Música en Espera':
                $data[$i-1]['MOH__events'] = trim($info);
                break;

              case 'Duración Música en Espera':
                $data[$i-1]['MOH_duration'] = trim($info);
                break;

              case 'Duración IVR':
                $data[$i-1]['IVR_duration'] = trim($info);
                break;

              case 'Ruta IVR':
                $data[$i-1]['IVR_path'] = trim($info);
                break;

              case 'Desconexión':
                $data[$i-1]['Desconexion'] = trim($info);
                break;

              case 'Transferida para':
              case '':
              case 'Tag':
              case 'Logro':
              case 'Variabiles':
                break;

              default:
                $data[$i-1][$field] = trim($info);
                break;
            }

          }

        }

        if( $i != 0 ){
            // Determine if Answered
            if( $data[$i-1]['Desconexion'] == 'Llamante' || $data[$i-1]['Desconexion'] == 'Agente' ){
              $data[$i-1]['Answered']=1;
              $total++;
            }else{
              $data[$i-1]['Answered']=0;
              $total++;
            }
        }

        $i++;
      }

      unset($data['-1']);
      fclose( $gestor );
      unlink($route.$fname);
    }

    return array( 'data' => $data, 'escaped' => $dataEscape, 'total' => $total );
  }

  public function setEndTime_get(){

    if( $this->db->query( "UPDATE t_Answered_Calls SET Hora_Fin = ADDTIME(Hora, ADDTIME(IVR, ADDTIME(Espera, Duracion) ) ) WHERE Last_Update >= CURDATE()" ) ){
      okResponse('Data procesada', 'data', true, $this);
    }else{
      errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }

  public function pauseCheck_get(){

    $this->db->query("DROP TEMPORARY TABLE IF EXISTS deletePauses");
    $this->db->query("CREATE TEMPORARY TABLE deletePauses SELECT * FROM asesores_pausas WHERE CAST(Inicio as DATE)>=ADDDATE(CURDATE(),-1)");
    $this->db->query("SELECT PAUSECALC(asesor, Inicio, Fin) as tmp FROM deletePauses");

      

    $this->response('OK');
  }
    
    public function grafPorAsesor_get(){
        
        $id = $this->uri->segment(3);
        $inicio = $this->uri->segment(4);
        $fin = $this->uri->segment(5);
        $asesor = $this->uri->segment(6);
        
        if( !isset($inicio) ){
            $inicio = "ADDDATE(CURDATE(),-2)";
            $fin = "ADDDATE(CURDATE(),-1)";
        }else{
            
            if( $inicio == "CURDATE"){
                $inicio = "CURDATE()";
                $fin = "CURDATE()";
                
            }else{
                $inicio = "'$inicio'";

                if( !isset($fin) ){
                    $fin = $inicio;
                }else{
                    $fin = "'$fin'";
                }
            }
            
        }
        
        if( $inicio == "CURDATE()" ){
            $hoteles = "t_hoteles_test";
            $locs = "d_Locs";
            $calls = "ccexporter.callsDetails";
        }else{
            $hoteles = "t_hoteles_test";
            $locs = "t_Locs";
            $calls = "t_Answered_Calls";
        }
        
        

      $this->db->query("SET @inicio = CAST($inicio as DATE)");
      $this->db->query("SET @fin = CAST($fin as DATE)");
        
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocs")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsB")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsC")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsOKPositive")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsOK")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS queryCalls")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS montos")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS querySesiones")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS cuartilOK")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("DROP TEMPORARY TABLE IF EXISTS locsServices")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        $this->db->select("a.*, IF(VentaMXN != 0, Localizador, NULL) AS NewLoc, dep", FALSE)
            ->from("$locs a")
            ->join("dep_asesores b", "a.asesor=b.asesor AND a.Fecha=b.Fecha")
            ->where("a.Fecha BETWEEN ", "@inicio AND @fin", FALSE)
            ->where("a.asesor >", 0)
            ->where("a.asesor IS NOT ", "NULL", FALSE);
        
        if( isset($asesor) ){
            $this->db->where("a.asesor", $asesor);
        }
        
        $q = $this->db->get_compiled_select();
        
        if(!$this->db->query("CREATE TEMPORARY TABLE queryLocs $q")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        if(!$this->db->query("ALTER TABLE queryLocs ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if(!$this->db->query("CREATE TEMPORARY TABLE queryLocsB SELECT * FROM queryLocs")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("ALTER TABLE queryLocsB ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if(!$this->db->query("CREATE TEMPORARY TABLE queryLocsC SELECT * FROM queryLocs")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        if(!$this->db->query("ALTER TABLE queryLocsC ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if(!$this->db->query("CREATE TEMPORARY TABLE queryLocsOK 
                    SELECT 
                        a.*,
                        FinalBalance,
                        IF(FinalBalance > 0 AND NewLoc IS NOT NULL,
                            NewLoc,
                            NULL) AS NewLocPositive,
                        IF(periodCreated IS NOT NULL,
                            a.Localizador,
                            NULL) AS periodCreated
                    FROM
                        queryLocs a
                            LEFT JOIN
                        (SELECT 
                            Localizador,
                                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS FinalBalance
                        FROM
                            queryLocsB
                        GROUP BY Localizador) b ON a.Localizador = b.Localizador
                            LEFT JOIN
                        (SELECT DISTINCT
                            NewLoc AS periodCreated
                        FROM
                            queryLocsC) c ON a.Localizador = c.periodCreated")){
            
            if(!$this->db->query("ALTER TABLE queryLocsOK ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN), ADD INDEX `newLoc` (`NewLocPositive` ASC)")){
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        if(!$this->db->query("CREATE TEMPORARY TABLE queryLocsOKPositive
                    SELECT 
                        locs_id, asesor, a.Localizador, Afiliado, Servicios, 
                        SUM(VentaMXN) as VentaMXN,
                        SUM(OtrosIngresosMXN) as OtrosIngresosMXN,
                        SUM(EgresosMXN) as EgresosMXN,
                        Fecha, chanId, tipo, NewLoc, dep
                        FinalBalance,
                        NewLocPositive,
                        periodCreated
                    FROM
                        queryLocsOK a
                    WHERE periodCreated IS NOT NULL
                    GROUP BY Fecha, periodCreated")){
            
            if(!$this->db->query("ALTER TABLE queryLocsOKPositive ADD PRIMARY KEY (Fecha, Localizador, VentaMXN), ADD INDEX `newLoc` (`NewLocPositive` ASC)")){
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        $this->db->select("a.Localizador,
                        a.item,
                        SUM(a.VentaMXN + a.OtrosIngresosMXN + a.EgresosMXN) AS Monto,
                        a.Fecha,
                        SUM(a.clientNights) AS RN,
                        CASE
                            WHEN a.isPaq != 0 THEN 'Paquete'
                            WHEN a.itemType = 1 THEN 'Hotel'
                            WHEN a.itemType = 3 THEN 'Vuelo'
                            WHEN a.itemType = 6 THEN 'Paquete'
                            WHEN
                                a.itemType = 14
                            THEN
                                CASE
                                    WHEN a.categoryId = 1 THEN 'Hotel'
                                    WHEN a.categoryId = 3 THEN 'Vuelo'
                                    WHEN a.categoryId = 0 THEN 'Paquete'
                                    ELSE 'Otros'
                                END
                            ELSE 'Otros'
                        END AS iType", FALSE)
            ->from("$hoteles a")
            ->where("a.Fecha BETWEEN ", "@inicio AND @fin", FALSE);
        
        if( isset($asesor) ){
            $this->db->join("(SELECT Localizador, asesor FROM queryLocs GROUP BY Localizador) b", "a.Localizador = b.Localizador", "left")
                ->where("b.asesor", $asesor);
        }
        
        $q = $this->db->get_compiled_select();
            


        if(!$this->db->query("CREATE TEMPORARY TABLE locsServices $q")){
            
            if(!$this->db->query("ALTER TABLE locsServices ADD PRIMARY KEY (Localizador , item , Fecha)")){
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        if( $inicio == "CURDATE()"){
            $query = "CREATE TEMPORARY TABLE queryCalls
                        SELECT 
                            GETIDASESOR(descr_agente, 2) AS asesor, CURDATE() as Fecha, Skill, 0 as xfered, COUNT(*) as calls, SUM(callLen) as TT
                        FROM
                            ccexporter.callsDetails a
                                LEFT JOIN
                            ccexporter.agentDetails b ON a.agent = b.nome_agente
                            LEFT JOIN Cola_Skill c ON a.queue=c.queue
                        WHERE
                            tstEnter > CURDATE()
                                AND callLen IS NOT NULL AND direction=1
                        GROUP BY asesor, Skill
                        HAVING asesor IS NOT NULL";
        }else{
            
            $this->db->select("asesor,
                        Fecha,
                        Skill,
                        COUNT(IF(Desconexion = 'Transferida'
                                AND Duracion_Real < '00:02:00',
                            ac_id,
                            NULL)) AS xfered,
                        COUNT(*) as calls, SUM(TIME_TO_SEC(Duracion_Real)) as TT", FALSE)
                ->from("t_Answered_Calls a")
                ->join("Cola_Skill b", "a.Cola = b.Cola", "left")
                ->where("Fecha BETWEEN ", "@inicio AND @fin", FALSE)
                ->where("asesor IS NOT ", "NULL", FALSE)
                ->where("direction", 1)
                ->group_by("asesor , Fecha , Skill");
            
            if( isset($asesor) ){
                $this->db->where("asesor", $asesor);
            }

            $q = $this->db->get_compiled_select();
            
            
            $query = "CREATE TEMPORARY TABLE queryCalls $q";
        }

        if(!$this->db->query($query)){
            
            if(!$this->db->query("ALTER TABLE queryCalls ADD PRIMARY KEY (asesor(20), Fecha , Skill)")){
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if(!$this->db->query("CREATE TEMPORARY TABLE montos        
                    SELECT 
                        a.asesor, a.Fecha, SUM(Monto) AS MontoTotal, 
                        SUM(IF(iType = 'Hotel', Monto,0)) as monto_hotel,
                        SUM(IF(iType = 'Paquete', Monto,0)) as monto_paquete,
                        SUM(IF(iType = 'Vuelo', Monto,0)) as monto_vuelo,
                        SUM(IF(iType = 'Otros', Monto,0)) as monto_otro,
                        COUNT(DISTINCT a.NewLocPositive) as rsvas_total
                    FROM
                        queryLocsOKPositive a
                            LEFT JOIN
                        locsServices b ON a.periodCreated = b.Localizador
                            AND a.Fecha = b.Fecha
                    GROUP BY a.Fecha , a.asesor")){
            
            if(!$this->db->query("ALTER TABLE montos ADD PRIMARY KEY (asesor, Fecha)")){
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        $this->db->where("Fecha BETWEEN ", "@inicio AND @fin", FALSE);
        
        if( isset($asesor) ){
            $this->db->where("asesor", $asesor);
        }
        
        if(!$this->db->delete("graf_ventaPorAsesor")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
        $this->db->select("a.asesor,
                        a.Fecha,
                        GETIDASESOR(FINDSUPDAY(a.asesor, a.Fecha), 2) AS supervisor,
                        GETIDASESOR(FINDSUPER(MONTH(a.Fecha), YEAR(a.Fecha), a.asesor), 2) AS supMes,
                        dep,
                        MontoTotal,
                        monto_hotel,
                        monto_paquete,
                        monto_vuelo,
                        monto_otro,
                        rsvas_total,
                        calls,
                        NULL AS llv,
                        NULL AS llh,
                        xfered,
                        TT,
                        NOW()", FALSE)
            ->from("dep_asesores a")
            ->join("montos b", "a.asesor = b.asesor AND a.Fecha=b.Fecha", "left")
            ->join("queryCalls c", "a.asesor = c.asesor AND a.Fecha=c.Fecha AND IF(a.dep = 29,35,a.dep) = c.Skill", "left", FALSE)
            ->where("a.Fecha BETWEEN ", "@inicio AND @fin", FALSE)
            ->where("vacante IS NOT ", "NULL", FALSE);
        
        if( isset($asesor) ){
            $this->db->where("a.asesor",$asesor);
        }
        
        $q = $this->db->get_compiled_select();

        if(!$this->db->query("INSERT INTO graf_ventaPorAsesor $q")){
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        


        if( $inicio != "CURDATE()" ){
            if( !isset($asesor) ){
                $this->db->query("UPDATE config_reportUpdates SET lastRun = NOW() WHERE id=$id");
            }
        }
        
        okResponse( 'Data Actualizada', 'data', true, $this );


      return true;


  }
    
    public function checkSupGraf_get(){
        
        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);
        
        if(isset($inicio)){
            $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
            if(isset($fin)){
                $this->db->query("SET @fin = CAST('$fin' as DATE)");
            }else{
                $this->db->query("SET @fin = CAST('$inicio' as DATE)");
            }
        }else{
            $this->db->query("SET @inicio = CAST(CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01') as DATE)");
            $this->db->query("SET @fin = CURDATE()");
        }

        $query = "UPDATE graf_dailySale a
                            LEFT JOIN
                        dep_asesores b ON a.asesor = b.asesor
                            AND a.Fecha = b.Fecha 
                    SET 
                        supervisor = IF(a.dep = 29,
                            FINDSUPERDAYPDV(a.Fecha, oficina, 0),
                            FINDSUPERDAYCC(a.Fecha, a.asesor, 0)),
                        supMes = IF(a.dep = 29,
                            FINDSUPERDAYPDV(DATE_ADD(DATE_ADD(CAST(CONCAT(YEAR(a.Fecha),
                                                        '-',
                                                        MONTH(a.Fecha),
                                                        '-01')
                                                AS DATE),
                                            INTERVAL 1 MONTH),
                                        INTERVAL - 1 DAY),
                                    oficina,
                                    0),
                            FINDSUPERDAYCC(DATE_ADD(DATE_ADD(CAST(CONCAT(YEAR(a.Fecha),
                                                        '-',
                                                        MONTH(a.Fecha),
                                                        '-01')
                                                AS DATE),
                                            INTERVAL 1 MONTH),
                                        INTERVAL - 1 DAY),
                                    a.asesor,
                                    0))
                    WHERE
                                a.Fecha BETWEEN @inicio AND @fin";

        if( $this->db->query($query) ){
            okResponse( 'Data Actualizada', 'data', true, $this );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        

  }
    
    
    public function rtLiveAgents_get(){
        
        if( $q = $this->db->query("SELECT json FROM ccexporter.rtMonitor WHERE tipo='RealtimeDO.RtAgentsRaw'") ){
            $data = $q->row_array();

            $result = str_replace("\"", "'", str_replace( " u'", " '", str_replace("[u'", "['", str_replace("{u'", "{'", $data['json']))));

            okResponse( 'Data', 'data', $result, $this );
        
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
    } 
    
    public function rtCalls_put(){
        
        $data = $this->put();
        
        $result = array();
        
        foreach($data['values'] as $index => $info){
            $tmp = array();
            foreach($info as $i => $x){
                $tmp[$data['fields'][$i]] = $x;
            }
            array_push( $result, $tmp );
        } 
        
        if( $this->db->insert_batch('ccexporter.rtCalls', $result) ){
            okResponse( 'Data', 'data', $data['fields'], $this );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
            
        
    }
    
    public function grafGen_get(){
        
        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);
        
        if(isset($inicio)){
            $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
            if(isset($fin)){
                $this->db->query("SET @fin = CAST('$fin' as DATE)");
            }else{
                $this->db->query("SET @fin = CAST('$inicio' as DATE)");
            }
        }else{
            $this->db->query("SET @inicio = CAST(CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01') as DATE)");
            $this->db->query("SET @fin = CURDATE()");
        }
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS asesoresDS");
        $this->db->query("CREATE TEMPORARY TABLE asesoresDS SELECT 
            Fecha as FechaDS, asesor as asesorDS, dep as depDS
        FROM
            dep_asesores
        WHERE
            Fecha BETWEEN @inicio AND @fin
                AND vacante IS NOT NULL");
        $this->db->query("ALTER TABLE asesoresDS ADD PRIMARY KEY (FechaDS, asesorDS)");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");
        
        $this->db->select("a.*, marca, gpoCanalKpi, pais, b.dep, tipoRsva, gpoTipoRsva")
            ->from('t_Locs a')
            ->join('dep_asesores b', 'a.asesor = b.asesor AND a.Fecha = b.Fecha', 'left')
            ->join('chanGroups c', 'a.chanId = c.id', 'left')
            ->join('config_tipoRsva d', 'IF(a.tipo IS NULL, 0, a.tipo) = d.tipo
                                        AND IF(b.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50), 0, b.dep)
                                            ) = d.dep', 'left', FALSE)
            ->where("a.Fecha BETWEEN @inicio AND @fin AND a.asesor >= 0", "", FALSE)
            ->where("vacante IS NOT NULL", " ", FALSE);
        
        $locs = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE locs $locs");
        $this->db->query("ALTER TABLE locs ADD PRIMARY KEY (Localizador, Fecha, Hora, Venta)");
        
        $this->db->select("a.*, marca, gpoCanalKpi, pais, b.dep, tipoRsva, gpoTipoRsva")
            ->from('d_Locs a')
            ->join('dep_asesores b', 'a.asesor = b.asesor AND a.Fecha = b.Fecha', 'left')
            ->join('chanGroups c', 'a.chanId = c.id', 'left')
            ->join('config_tipoRsva d', 'IF(a.tipo IS NULL, 0, a.tipo) = d.tipo
                                        AND IF(b.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50), 0, b.dep)
                                            ) = d.dep', 'left', FALSE)
            ->where("a.Fecha BETWEEN @inicio AND @fin AND a.asesor >= 0", "", FALSE)
            ->where("vacante IS NOT NULL", " ", FALSE);
        
        $locsTD = $this->db->get_compiled_select();
        $this->db->query("INSERT INTO locs SELECT * FROM ($locsTD) a ON DUPLICATE KEY UPDATE asesor=a.asesor, tipo=a.tipo, tipoRsva=a.tipoRsva, gpoTipoRsva=a.gpoTipoRsva");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsDate");
        
        $this->db->select("Fecha, Localizador, asesor, marca, gpoCanalKpi, pais, dep, tipoRsva, gpoTipoRsva")
            ->select("SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                        IF(SUM(VentaMXN) > 0, Localizador, NULL) AS NewLoc,
                        IF(SUM(VentaMXN) > 0 AND gpoTipoRsva = 'In', Localizador, NULL) AS NewLocIn,
                        IF(SUM(VentaMXN) > 0 AND gpoTipoRsva != 'In', Localizador, NULL) AS NewLocNotIn,
                        IF(SUM(VentaMXN) >= 0
                                AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                            Localizador,
                            NULL) AS CountLoc,
                        IF(SUM(VentaMXN) > 0
                                OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                            Localizador,
                            NULL) AS ModifLoc", FALSE)
            ->from("locs")

            // No cuenta canal Outlet en fechas Outlet
            // ->not_group_start()
            //     ->where('Fecha BETWEEN', ' \'2018-05-10\' AND \'2018-05-13\'', FALSE)
            //     ->where('gpoCanalKpi', 'Outlet')
            // ->group_end()

            ->group_by(array("Fecha", "Localizador"));
        $locsDate = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE locsDate $locsDate");
        $this->db->query("ALTER TABLE locsDate ADD PRIMARY KEY (Localizador, Fecha)");

        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS byAsesor");
        
        $this->db->select("a.Fecha, a.Localizador, asesor, dep, servicio, isPaq, marca, gpoCanalKpi, pais, tipoRsva, gpoTipoRsva, NewLocIn, NewLocNotIn")
            ->select("SUM(IF(CountLoc IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) AS MontoSV, SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS MontoAll", FALSE)
            ->select("SUM(IF(CountLoc IS NOT NULL, costo, 0)) AS MargenSV, SUM(costo) AS MargenAll", FALSE)
            ->from("t_hoteles_test a")
            ->join("t_margen m", "a.Localizador = m.Localizador AND a.item = m.item AND a.Fecha = m.Fecha AND a.Hora = m.Hora", "left")
            ->join("locsDate b", "a.Localizador = b.Localizador AND a.Fecha = b.Fecha", "right")
            ->join("itemTypes c", "a.itemType = c.type AND a.categoryId = c.category", "left")
            ->where("a.Localizador IS NOT ", "NULL", FALSE)
            ->group_by(array("a.Fecha", "a.Localizador", "asesor", "servicio", "gpoTipoRsva"));
        
        $byAsesor = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE byAsesor $byAsesor");
        $this->db->query("ALTER TABLE byAsesor ADD PRIMARY KEY (Fecha, Localizador, asesor, servicio, gpoTipoRsva)");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS byAsesorOK");
        $this->db->query("CREATE TEMPORARY TABLE byAsesorOK
            SELECT * FROM asesoresDS a LEFT JOIN byAsesor b ON a.FechaDS=b.Fecha AND a.asesorDS=b.asesor");
        $this->db->query("ALTER TABLE byAsesorOK ADD PRIMARY KEY (FechaDS, Localizador, asesorDS, servicio, gpoTipoRsva)");

        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS final");
        
        $this->db->select("FechaDS as Fecha,
                        asesorDS as asesor, NULL as sup, NULL as supMes,
                        depDS,

                        SUM(MontoSV) AS MontoSV,
                        SUM(MontoAll) AS MontoAll,

                        SUM(MargenSV) AS MargenSV,
                        SUM(MargenAll) AS MargenAll,

                        COUNT(DISTINCT NewLocIn) as LocsIn,
                        SUM(IF(gpoTipoRsva = 'In', MontoSV, 0)) AS MontoInSV,
                        SUM(IF(gpoTipoRsva = 'In', MontoAll, 0)) AS MontoInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Hotel' AND gpoTipoRsva = 'In', MontoSV, 0)) AS HotelInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Hotel' AND gpoTipoRsva = 'In', MontoAll, 0)) AS HotelInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Vuelo' AND gpoTipoRsva = 'In', MontoSV, 0)) AS VueloInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Vuelo' AND gpoTipoRsva = 'In', MontoAll, 0)) AS VueloInAll,

                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Transfer' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS TransferInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Transfer' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS TransferInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Tour' 		AND gpoTipoRsva = 'In', MontoSV, 0)) AS TourInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Tour' 		AND gpoTipoRsva = 'In', MontoAll, 0)) AS TourInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Crucero' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS CruceroInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Crucero' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS CruceroInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Circuito' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS CircuitoInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Circuito' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS CircuitoInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Seguro' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS SeguroInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Seguro' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS SeguroInAll,

                        SUM(IF(servicio = 'Hotel' AND gpoTipoRsva = 'In', MontoSV, 0)) AS HotelAllInSV,
                        SUM(IF(servicio = 'Hotel' AND gpoTipoRsva = 'In', MontoAll, 0)) AS HotelAllInAll,
                        SUM(IF(servicio = 'Vuelo' AND gpoTipoRsva = 'In', MontoSV, 0)) AS VueloAllInSV,
                        SUM(IF(servicio = 'Vuelo' AND gpoTipoRsva = 'In', MontoAll, 0)) AS VueloAllInAll,

                        SUM(IF(servicio = 'Transfer' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS TransferAllInSV,
                        SUM(IF(servicio = 'Transfer' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS TransferAllInAll,
                        SUM(IF(servicio = 'Tour' 		AND gpoTipoRsva = 'In', MontoSV, 0)) AS TourAllInSV,
                        SUM(IF(servicio = 'Tour' 		AND gpoTipoRsva = 'In', MontoAll, 0)) AS TourAllInAll,
                        SUM(IF(servicio = 'Crucero' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS CruceroAllInSV,
                        SUM(IF(servicio = 'Crucero' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS CruceroAllInAll,
                        SUM(IF(servicio = 'Circuito' 	AND gpoTipoRsva = 'In', MontoSV, 0)) AS CircuitoAllInSV,
                        SUM(IF(servicio = 'Circuito' 	AND gpoTipoRsva = 'In', MontoAll, 0)) AS CircuitoAllInAll,
                        SUM(IF(servicio = 'Seguro' 		AND gpoTipoRsva = 'In', MontoSV, 0)) AS SeguroAllInSV,
                        SUM(IF(servicio = 'Seguro' 		AND gpoTipoRsva = 'In', MontoAll, 0)) AS SeguroAllInAll,

                        SUM(IF(COALESCE(isPaq,0) != 0 AND gpoTipoRsva = 'In', MontoSV, 0)) AS PaqueteInSV,
                        SUM(IF(COALESCE(isPaq,0) != 0 AND gpoTipoRsva = 'In', MontoAll, 0)) AS PaqueteInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio NOT IN ('Hotel' , 'Vuelo', 'Paquete', 'Transfer', 'Tour', 'Crucero', 'Circuito', 'Seguro') AND gpoTipoRsva = 'In',
                            MontoSV,
                            0)) AS OtrosInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio NOT IN ('Hotel' , 'Vuelo', 'Paquete', 'Transfer', 'Tour', 'Crucero', 'Circuito', 'Seguro') AND gpoTipoRsva = 'In',
                            MontoAll,
                            0)) AS OtrosInAll,

                        COUNT(DISTINCT NewLocNotIn) as LocsNotIn,
                        SUM(IF(gpoTipoRsva != 'In', MontoSV, 0)) AS MontoNotInSV,
                        SUM(IF(gpoTipoRsva != 'In', MontoAll, 0)) AS MontoNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Hotel' AND gpoTipoRsva != 'In', MontoSV, 0)) AS HotelNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Hotel' AND gpoTipoRsva != 'In', MontoAll, 0)) AS HotelNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Vuelo' AND gpoTipoRsva != 'In', MontoSV, 0)) AS VueloNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Vuelo' AND gpoTipoRsva != 'In', MontoAll, 0)) AS VueloNotInAll,

                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Transfer' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS TransferNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Transfer' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS TransferNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Tour' 		AND gpoTipoRsva != 'In', MontoSV, 0)) AS TourNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Tour' 		AND gpoTipoRsva != 'In', MontoAll, 0)) AS TourNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Crucero' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS CruceroNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Crucero' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS CruceroNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Circuito' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS CircuitoNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Circuito' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS CircuitoNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Seguro' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS SeguroNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio = 'Seguro' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS SeguroNotInAll,

                        SUM(IF(servicio = 'Hotel' AND gpoTipoRsva != 'In', MontoSV, 0)) AS HotelAllNotInSV,
                        SUM(IF(servicio = 'Hotel' AND gpoTipoRsva != 'In', MontoAll, 0)) AS HotelAllNotInAll,
                        SUM(IF(servicio = 'Vuelo' AND gpoTipoRsva != 'In', MontoSV, 0)) AS VueloAllNotInSV,
                        SUM(IF(servicio = 'Vuelo' AND gpoTipoRsva != 'In', MontoAll, 0)) AS VueloAllNotInAll,

                        SUM(IF(servicio = 'Transfer' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS TransferAllNotInSV,
                        SUM(IF(servicio = 'Transfer' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS TransferAllNotInAll,
                        SUM(IF(servicio = 'Tour' 		AND gpoTipoRsva != 'In', MontoSV, 0)) AS TourAllNotInSV,
                        SUM(IF(servicio = 'Tour' 		AND gpoTipoRsva != 'In', MontoAll, 0)) AS TourAllNotInAll,
                        SUM(IF(servicio = 'Crucero' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS CruceroAllNotInSV,
                        SUM(IF(servicio = 'Crucero' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS CruceroAllNotInAll,
                        SUM(IF(servicio = 'Circuito' 	AND gpoTipoRsva != 'In', MontoSV, 0)) AS CircuitoAllNotInSV,
                        SUM(IF(servicio = 'Circuito' 	AND gpoTipoRsva != 'In', MontoAll, 0)) AS CircuitoAllNotInAll,
                        SUM(IF(servicio = 'Seguro' 		AND gpoTipoRsva != 'In', MontoSV, 0)) AS SeguroAllNotInSV,
                        SUM(IF(servicio = 'Seguro' 		AND gpoTipoRsva != 'In', MontoAll, 0)) AS SeguroAllNotInAll,

                        SUM(IF(COALESCE(isPaq,0) != 0 AND gpoTipoRsva != 'In', MontoSV, 0)) AS PaqueteNotInSV,
                        SUM(IF(COALESCE(isPaq,0) != 0 AND gpoTipoRsva != 'In', MontoAll, 0)) AS PaqueteNotInAll,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio NOT IN ('Hotel' , 'Vuelo', 'Paquete', 'Transfer', 'Tour', 'Crucero', 'Circuito', 'Seguro') AND gpoTipoRsva != 'In',
                            MontoSV,
                            0)) AS OtrosNotInSV,
                        SUM(IF(COALESCE(isPaq,0) = 0 AND servicio NOT IN ('Hotel' , 'Vuelo', 'Paquete', 'Transfer', 'Tour', 'Crucero', 'Circuito', 'Seguro') AND gpoTipoRsva != 'In',
                            MontoAll,
                            0)) AS OtrosNotInAll, 0 as callsin, 0 as ttin, 0 as ahtin, 0 as callsout,0 as ttout, 0 as ahtout, 0 as intentosout, NULL as LU", FALSE)
            ->from("byAsesorOK")
            ->group_by(array("FechaDS", "asesorDS"));
        
        $final = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE final $final");

        if( $this->db->query("INSERT INTO graf_dailySale (SELECT * FROM (SELECT * FROM final) a) ON DUPLICATE KEY UPDATE
                                MontoSV = COALESCE(a.MontoSV, 0),
                                MontoAll = COALESCE(a.MontoAll, 0),
                                MargenSV = COALESCE(a.MargenSV, 0),
                                MargenAll = COALESCE(a.MargenAll, 0),
                                LocsIn = COALESCE(a.LocsIn, 0),
                                MontoInSV = COALESCE(a.MontoInSV, 0),
                                MontoInAll = COALESCE(a.MontoInAll, 0),
                                HotelInSV = COALESCE(a.HotelInSV, 0),
                                HotelInAll = COALESCE(a.HotelInAll, 0),
                                VueloInSV = COALESCE(a.VueloInSV, 0),
                                VueloInAll = COALESCE(a.VueloInAll, 0),
                                TransferInSV = COALESCE(a.TransferInSV, 0),
                                TransferInAll = COALESCE(a.TransferInAll, 0),
                                TourInSV = COALESCE(a.TourInSV, 0),
                                TourInAll = COALESCE(a.TourInAll, 0),
                                CruceroInSV = COALESCE(a.CruceroInSV, 0),
                                CruceroInAll = COALESCE(a.CruceroInAll, 0),
                                CircuitoInSV = COALESCE(a.CircuitoInSV, 0),
                                CircuitoInAll = COALESCE(a.CircuitoInAll, 0),
                                SeguroInSV = COALESCE(a.SeguroInSV, 0),
                                SeguroInAll = COALESCE(a.SeguroInAll, 0),
                                HotelAllInSV = COALESCE(a.HotelAllInSV, 0),
                                HotelAllInAll = COALESCE(a.HotelAllInAll, 0),
                                VueloAllInSV = COALESCE(a.VueloAllInSV, 0),
                                VueloAllInAll = COALESCE(a.VueloAllInAll, 0),
                                TransferAllInSV = COALESCE(a.TransferAllInSV, 0),
                                TransferAllInAll = COALESCE(a.TransferAllInAll, 0),
                                TourAllInSV = COALESCE(a.TourAllInSV, 0),
                                TourAllInAll = COALESCE(a.TourAllInAll, 0),
                                CruceroAllInSV = COALESCE(a.CruceroAllInSV, 0),
                                CruceroAllInAll = COALESCE(a.CruceroAllInAll, 0),
                                CircuitoAllInSV = COALESCE(a.CircuitoAllInSV, 0),
                                CircuitoAllInAll = COALESCE(a.CircuitoAllInAll, 0),
                                SeguroAllInSV = COALESCE(a.SeguroAllInSV, 0),
                                SeguroAllInAll = COALESCE(a.SeguroAllInAll, 0),
                                PaqueteInSV = COALESCE(a.PaqueteInSV, 0),
                                PaqueteInAll = COALESCE(a.PaqueteInAll, 0),
                                OtrosInSV = COALESCE(a.OtrosInSV, 0),
                                OtrosInAll = COALESCE(a.OtrosInAll, 0),
                                LocsNotIn = COALESCE(a.LocsNotIn, 0),
                                MontoNotInSV = COALESCE(a.MontoNotInSV, 0),
                                MontoNotInAll = COALESCE(a.MontoNotInAll, 0),
                                HotelNotInSV = COALESCE(a.HotelNotInSV, 0),
                                HotelNotInAll = COALESCE(a.HotelNotInAll, 0),
                                VueloNotInSV = COALESCE(a.VueloNotInSV, 0),
                                VueloNotInAll = COALESCE(a.VueloNotInAll, 0),
                                TransferNotInSV = COALESCE(a.TransferNotInSV, 0),
                                TransferNotInAll = COALESCE(a.TransferNotInAll, 0),
                                TourNotInSV = COALESCE(a.TourNotInSV, 0),
                                TourNotInAll = COALESCE(a.TourNotInAll, 0),
                                CruceroNotInSV = COALESCE(a.CruceroNotInSV, 0),
                                CruceroNotInAll = COALESCE(a.CruceroNotInAll, 0),
                                CircuitoNotInSV = COALESCE(a.CircuitoNotInSV, 0),
                                CircuitoNotInAll = COALESCE(a.CircuitoNotInAll, 0),
                                SeguroNotInSV = COALESCE(a.SeguroNotInSV, 0),
                                SeguroNotInAll = COALESCE(a.SeguroNotInAll, 0),
                                HotelAllNotInSV = COALESCE(a.HotelAllNotInSV, 0),
                                HotelAllNotInAll = COALESCE(a.HotelAllNotInAll, 0),
                                VueloAllNotInSV = COALESCE(a.VueloAllNotInSV, 0),
                                VueloAllNotInAll = COALESCE(a.VueloAllNotInAll, 0),
                                TransferAllNotInSV = COALESCE(a.TransferAllNotInSV, 0),
                                TransferAllNotInAll = COALESCE(a.TransferAllNotInAll, 0),
                                TourAllNotInSV = COALESCE(a.TourAllNotInSV, 0),
                                TourAllNotInAll = COALESCE(a.TourAllNotInAll, 0),
                                CruceroAllNotInSV = COALESCE(a.CruceroAllNotInSV, 0),
                                CruceroAllNotInAll = COALESCE(a.CruceroAllNotInAll, 0),
                                CircuitoAllNotInSV = COALESCE(a.CircuitoAllNotInSV, 0),
                                CircuitoAllNotInAll = COALESCE(a.CircuitoAllNotInAll, 0),
                                SeguroAllNotInSV = COALESCE(a.SeguroAllNotInSV, 0),
                                SeguroAllNotInAll = COALESCE(a.SeguroAllNotInAll, 0),
                                PaqueteNotInSV = COALESCE(a.PaqueteNotInSV, 0),
                                PaqueteNotInAll = COALESCE(a.PaqueteNotInAll, 0),
                                OtrosNotInSV = COALESCE(a.OtrosNotInSV, 0),
                                OtrosNotInAll = COALESCE(a.OtrosNotInAll, 0)") ){

            okResponse( 'Información Actualizada', 'data', TRUE, $this, 'inicio', $fin );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
    }
    
    public function genGrafCalls_get(){
        
        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);
        
        if(isset($inicio)){
            $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
            if(isset($fin)){
                $this->db->query("SET @fin = CAST('$fin' as DATE)");
            }else{
                $this->db->query("SET @fin = CAST('$inicio' as DATE)");
            }
        }else{
            $this->db->query("SET @inicio = CAST(CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01') as DATE)");
            $this->db->query("SET @fin = CURDATE()");
        }
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS asesoresDS");
        $this->db->query("CREATE TEMPORARY TABLE asesoresDS SELECT 
            Fecha as FechaDS, asesor as asesorDS, dep as depDS
        FROM
            dep_asesores
        WHERE
            Fecha BETWEEN @inicio AND @fin
                AND vacante IS NOT NULL");
        $this->db->query("ALTER TABLE asesoresDS ADD PRIMARY KEY (FechaDS, asesorDS)");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS calls");
        
        $this->db->select("AsteriskId as id, Fecha, Hora, Llamante, a.Cola, Espera, Desconexion, asesor, DNIS, Duracion_Real, Answered, Skill, direction, Last_Update")
            ->from('t_Answered_Calls a')
            ->join('Cola_Skill b', 'a.Cola = b.Cola AND b.active = 1', 'left')
            ->where("a.Fecha BETWEEN @inicio AND @fin AND a.asesor >= 0", "", FALSE)
            ->having("Skill IS NOT NULL", "", FALSE);
        
        $calls = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE calls $calls");
        $this->db->query("ALTER TABLE calls ADD PRIMARY KEY (id(20))");
        
        $this->db->select("callid as id,
                            CAST(tstEnter AS DATE) AS Fecha,
                            CAST(tstEnter AS TIME) AS Hora,
                            `from` AS Llamante,
                            Cola,
                            SEC_TO_TIME(waitLen) AS Espera,
                            IF(callLen IS NULL,
                                'Abandono',
                                'Answered') AS Desconexion,
                            GETIDASESOR(descr_agente, 2) AS asesor,
                            CONCAT(IF(SUBSTRING(dnis, 1, 1) = '0',
                                        '\'',
                                        ''),
                                    dnis) AS DNIS,
                            SEC_TO_TIME(callLen) AS dur,
                            IF(COALESCE(callLen,0)=0, 0, 1) AS Answered,
                            Skill, direction,
                            a.Last_Update", FALSE)
            ->from('ccexporter.callsDetails a')
            ->join('Cola_Skill b', 'a.queue = b.queue AND b.active = 1', 'left')
            ->join('ccexporter.agentDetails c ', 'IF(COALESCE(callLen,0)=0, a.agentOut, a.agent) = nome_agente', 'left', FALSE)
            ->where("tstEnter >= CURDATE()", "", FALSE)
            ->having("Skill IS NOT NULL", "", FALSE);
        
        $callsTD = $this->db->get_compiled_select();
        
        $this->db->query("INSERT INTO calls SELECT * FROM ($callsTD) a ON DUPLICATE KEY UPDATE Duracion_real=a.dur, Answered=a.Answered, Espera=a.Espera, asesor=a.asesor");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS sumCalls");
        
        $this->db->select("Fecha,
                            asesor,
                            Skill,
                            COUNT(IF(NOT (Desconexion != 'Transferida'
                                    AND TIME_TO_SEC(Duracion_Real) < 120) AND direction = 1,
                                id,
                                NULL)) AS callsIn,
                            SUM(IF(direction = 1 AND Answered=1, COALESCE(TIME_TO_SEC(Duracion_Real),0), 0)) AS TTIn,
                            COUNT(IF(Answered=1 AND direction = 2,
                                id,
                                NULL)) AS callsOut,
                            SUM(IF(direction = 2 AND Answered=1, COALESCE(TIME_TO_SEC(Duracion_Real),0), 0)) AS TTOut,
                            COUNT(IF(Answered=0 AND direction = 2,
                                id,
                                NULL)) AS intentosOut", FALSE)
            ->from('calls')
            ->group_by(array("Fecha" , "asesor" , "Skill"))
            ->having("asesor IS NOT NULL", "", FALSE);
        
        
        
        $sumCalls = $this->db->get_compiled_select();
        
        $this->db->query("CREATE TEMPORARY TABLE sumCalls $sumCalls");
        $this->db->query("ALTER TABLE sumCalls ADD PRIMARY KEY (Fecha, asesor(20), Skill)");
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS final");
        
        $this->db->select("FechaDS as Fecha, asesorDS as asesor, 
                        Skill, 
                        COALESCE(callsIn,0) as callsIn, 
                        COALESCE(TTIn,0) as TTIn, 
                        IF(COALESCE(callsIn,0) = 0, 0, COALESCE(TTIn,0)/COALESCE(callsIn,0)) as AHTIn, 
                        COALESCE(callsOut,0) as callsOut, 
                        COALESCE(TTOut,0) as TTOut, 
                        IF(COALESCE(callsOut,0) = 0, 0, COALESCE(TTOut,0)/COALESCE(callsOut,0)) as AHTOut,
                        COALESCE(intentosOut,0) as intentosOut", FALSE)
            ->from('asesoresDS a')
            ->join('sumCalls b', 'FechaDS=Fecha AND asesorDS=asesor', 'left');
        
        $final = $this->db->get_compiled_select();
        $this->db->query("CREATE TEMPORARY TABLE final $final");
        $this->db->query("ALTER TABLE final ADD PRIMARY KEY (Fecha, asesor, Skill)");

        
        if( $this->db->query("UPDATE graf_dailySale a
                                    LEFT JOIN
                                final b ON a.Fecha = b.Fecha
                                    AND a.asesor = b.asesor
                                    AND IF(a.dep IN (35, 29), 35, a.dep) = b.Skill 
                            SET 
                                a.callsIn = b.callsIn,
                                a.TTIn = b.TTin,
                                a.AHTIn = b.AHTIn,
                                a.callsOut = b.callsOut,
                                a.TTOut = b.TTOut,
                                a.AHTOut = b.AHTOut,
                                a.intentosOut = b.intentosOut
                            WHERE
                                a.Fecha BETWEEN @inicio AND @fin") ){
           okResponse( 'Información Actualizada', 'data', TRUE, $this);
        }else{
           errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        
    }
    
    public function uploadFcr_put(){
        
        $data = $this->put();
        
        $ans    = 0;
        $ansOK  = 0;
        $ansERR = 0;
        
        foreach( $data as $index => $info){
            
            $asesor = $info['smd_id'];

            $this->db->set($info)
                    ->set('asesor',"IF(GETIDASESOR('$asesor',3) IS NULL, 0, GETIDASESOR('$asesor',3))", FALSE);


            $query = $this->db->get_compiled_insert("asesores_fcr")." ON DUPLICATE KEY UPDATE fcr = ".$info['fcr'].", bi = ".$info['bi'];

            if( $this->db->query($query) ){
              $resultado[$index] = array( 'status' => TRUE, 'msg' => '');
              $ansOK++;
            }else{
              $resultado[$index] = array( 'status' => FALSE, 'msg' => $this->db->error(), 'query' => $query, 'info' => $info );
              $ansERR++;
            }
            
            $ans++;
        }
        
        $res = array(
                  'Registros'           => $ans,
                  'UPL_OK'              => $ansOK,
                  'UPL_ERR'             => $ansERR,
                  'data'                => $resultado
                );

        okResponse('Data procesada', 'data', $res, $this);

        
    }
    
    public function uploadPec_put(){
        
        $data = $this->put();
        
        $ans    = 0;
        $ansOK  = 0;
        $ansERR = 0;
        
        foreach( $data as $index => $info){
            
            $asesor = $info['smd_id'];

            $this->db->set($info)
                    ->set('asesor',"IF(GETIDASESOR('$asesor',3) IS NULL, 0, GETIDASESOR('$asesor',3))", FALSE);


            $query = $this->db->get_compiled_insert("asesores_pec")." ON DUPLICATE KEY UPDATE ecn = ".$info['ecn'].", ecuf = ".$info['ecuf'].", ecc = ".$info['ecc'];

            if( $this->db->query($query) ){
              $resultado[$index] = array( 'status' => TRUE, 'msg' => '');
              $ansOK++;
            }else{
              $resultado[$index] = array( 'status' => FALSE, 'msg' => $this->db->error(), 'query' => $query, 'info' => $info );
              $ansERR++;
            }
            
            $ans++;
        }
        
        $res = array(
                  'Registros'           => $ans,
                  'UPL_OK'              => $ansOK,
                  'UPL_ERR'             => $ansERR,
                  'data'                => $resultado
                );

        okResponse('Data procesada', 'data', $res, $this);

        
    }
    
    public function hxCompleted_get(){
        
        $query = "UPDATE asesores_logs a
                                RIGHT JOIN
                            asesores_programacion b ON a.asesor = b.asesor
                                AND ((login < x1e AND logout > x1s)
                                OR (login < x2e AND logout > x2s)) 
                        SET 
                            phx_done = IF(COALESCE(TIME_TO_SEC(TIMEDIFF(IF(login < x1e AND logout > x1s,
                                                        IF(logout > x1e, x1e, logout),
                                                        NULL),
                                                    IF(login < x1e AND logout > x1s,
                                                        IF(login < x1s, x1s, login),
                                                        NULL))) / 60 / 60,
                                    0) + COALESCE(TIME_TO_SEC(TIMEDIFF(IF(login < x2e AND logout > x2s,
                                                        IF(logout > x2e, x2e, logout),
                                                        NULL),
                                                    IF(login < x2e AND logout > x2s,
                                                        IF(login < x2s, x2s, login),
                                                        NULL))) / 60 / 60,
                                    0) > COALESCE(TIMEDIFF(x1e, x1s),0) + COALESCE(TIMEDIFF(x2e, x2s),0), COALESCE(TIMEDIFF(x1e, x1s),0) + COALESCE(TIMEDIFF(x2e, x2s),0),
                                    COALESCE(TIME_TO_SEC(TIMEDIFF(IF(login < x1e AND logout > x1s,
                                                        IF(logout > x1e, x1e, logout),
                                                        NULL),
                                                    IF(login < x1e AND logout > x1s,
                                                        IF(login < x1s, x1s, login),
                                                        NULL))) / 60 / 60,
                                    0) + COALESCE(TIME_TO_SEC(TIMEDIFF(IF(login < x2e AND logout > x2s,
                                                        IF(logout > x2e, x2e, logout),
                                                        NULL),
                                                    IF(login < x2e AND logout > x2s,
                                                        IF(login < x2s, x2s, login),
                                                        NULL))) / 60 / 60,
                                    0))
                        WHERE
                            (x1s != x1e OR x2s != x2e)
                            AND x1s >= ADDDATE(CURDATE(),-60)";
        
        if( $this->db->query($query) ){
            okResponse( 'Data Actualizada', 'data', true, $this );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        

  }
    
    public function dtCompleted_get(){
        
        $query = "UPDATE asesores_ausentismos a
                            LEFT JOIN
                        asesores_programacion b ON a.asesor = b.asesor
                            AND a.Fecha = b.Fecha 
                    SET 
                        pdt_done = TIME_TO_SEC(TIMEDIFF(IF(CHECKLOG(a.Fecha, a.asesor, 'out') > je,
                                            je,
                                            CHECKLOG(a.Fecha, a.asesor, 'out')),
                                        IF(CHECKLOG(a.Fecha, a.asesor, 'in') < js,
                                            js,
                                            CHECKLOG(a.Fecha, a.asesor, 'in')))) / TIME_TO_SEC(TIMEDIFF(je, js)) * 8
                    WHERE
                        ausentismo = 19 AND a = 1
                            AND a.Fecha >= ADDDATE(CURDATE(), - 60)";
        
        if( $this->db->query($query) ){
            okResponse( 'Data Actualizada', 'data', true, $this );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
        

  }
    

    public function uploadPdvLogs_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();
            $okQ = array();
            $errQ = array();
            
            foreach($data as $log => $info){
                $this->db->set($info);
                $q = $this->db->get_compiled_insert('asesores_logs');
                if($this->db->query("$q ON DUPLICATE KEY UPDATE asesor=".$info['asesor'])){
                    array_push($okQ, array('id' => $info['asesor'].".".$info['Fecha'], 'status' => true));
                }else{
                    array_push($errQ, array('id' => $info['asesor'].".".$info['Fecha'], 'status' => false, 'error' => $this->db->error()));
                }
            }
                
            okResponse( 'Info Obtenida', 'data', array( 'ok' => count($okQ), 'error' => count($errQ), 'errores' => $errQ), $this);

        });
        jsonPrint( $result );
    }

}
