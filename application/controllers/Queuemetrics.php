<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Queuemetrics extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function rtMonitor_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $block = $this->post();

      $this->db->select('tipo, json')
              ->select('Last_update')
              ->from('ccexporter.rtMonitor', false)
              ->where_in( 'tipo', $block );

      if( $q = $this->db->get() ){

        okResponse( $block." obtenido", 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }
    
  public function pbxStatus_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();


      $this->db->select('tipo, json')
              ->select('Last_update')
              ->from('ccexporter.rtMonitor', false)
              ->where_in( 'tipo', $data['block'] );

      if( $q = $this->db->get() ){
          
        $this->db->select("a.asesor,
                            IF(COALESCE(correctPauseType, tipo)=3,'Comida',IF(COALESCE(correctPauseType, tipo)=11,'PNP','Otros')) AS tipoPausa,
                            SUM(IF(COALESCE(b.status,0) != 1,
                                TIME_TO_SEC(Duracion),
                                0))/60 AS Total", FALSE) 
            ->from('asesores_pausas a')
            ->join('asesores_pausas_status b', 'a.id=b.id', 'left')
            ->where( 'inicio >= ', 'CURDATE()', FALSE)
            ->where( 'a.asesor', $_GET['usid'])
//            ->where( 'a.asesor', 31)
            ->group_by(array('a.asesor', 'tipoPausa'));
            
        $pausa = $this->db->get();
        $pausas = array();
          
        foreach( $pausa->result_array() as $index => $info ){
            $pausas[$info['tipoPausa']]=floatVal($info['Total']);
        }
        okResponse( $data['block']." obtenido", 'data', $q->result_object(), $this, 'pausas', $pausas );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function queues_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('Skill, Cola, queue, Departamento, monShow, direction, displaySum, sede')
              ->from('Cola_Skill a')
              ->join('PCRCs b', 'a.monShow = b.id', 'left')
              ->where('active', '1')
              ->where('sede IS NOT NULL', '', FALSE)
              ->order_by('Departamento');


      if( $q = $this->db->get() ){

        okResponse( "Colas obtenidas", 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }
    
  public function pauses_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('*')
              ->from('Tipos_pausas');


      if( $q = $this->db->get() ){
          
          foreach($q->result_array() as $index => $pause){
              $result[$pause['pausa_id']] = $pause;
          }

        okResponse( "Pausas obtenidas", 'data', $result, $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function asesorDep_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('a.asesor, dep, Departamento')
              ->select('NOMBREASESOR(a.asesor,1) as name', FALSE)
              ->select('IF(cc IS NULL, Departamento, CONCAT(\'PDV \',cc)) as depCC', FALSE)
              ->select('IF(cc IS NULL, color, \'#27b724\') as color', FALSE)
              ->from('dep_asesores a')
              ->join('PCRCs b', 'a.dep = b.id', 'left')
              ->join('cc_apoyo c', 'a.asesor = c.asesor AND CURDATE() BETWEEN inicio AND fin', 'left', FALSE)
              ->where('Fecha = ', 'CURDATE()', FALSE)
              ->where('vacante IS NOT ', 'NULL', FALSE);


      if( $q = $this->db->get() ){

        okResponse( "Deps asesores obtenidos", 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function pauseMon_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $date = $this->uri->segment(3);
      segmentSet(  3, "Debe incluir una fecha", $this );
      segmentType( 3, "Debe incluir una fecha en formato YYYY-MM-DD", $this, $type = 'date' );

      $this->db->select('a.*,
                        NOMBREASESOR(a.asesor, 2) AS Nombre,
                        NOMBREDEP(dep) AS Departamento,
                        Pausa,
                        TIME_TO_SEC(Duracion) AS dur_seconds', FALSE)
              ->from('asesores_pausas a')
              ->join('dep_asesores b', 'a.asesor = b.asesor', 'left')
              ->join('Tipos_pausas c', 'a.tipo = c.pausa_id', 'left')
              ->where('Inicio >=', $date);


      if( $q = $this->db->get() ){

        okResponse( "Pausas obtenidas", 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function colgadasTB_get(){
    $r = $this->db->query("SELECT 
                          COALESCE(descr_agente, a.agent) AS Agente,
                          a.from AS Llamante,
                          tstEnd AS Finalizada,
                          SEC_TO_TIME(callLen) AS Duracion
                      FROM
                          ccexporter.callsDetails a
                              LEFT JOIN
                          Cola_Skill b ON a.queue = b.queue
                              LEFT JOIN
                          ccexporter.agentDetails c ON a.agent = c.nome_agente
                      WHERE
                          server LIKE '%avt%'
                              AND tstEnter >= CURDATE()
                              AND reason = 'A'
                              AND direction = 1
                              AND callLen IS NOT NULL
                      ORDER BY tstEnd DESC");
    
    $data = $r->result_array();

      echo "<script type='text/javascript'>
                setTimeout(function(){
                location = ''
              },180000)
            </script>";
    echo "<body><table><thead><tr><th>Agente</th><th>Llamante</th><th>Finalizada</th><th>Duracion</th></tr></thead><tbody>";

    foreach($data as $index => $info){
      echo "<tr><td style='border: 1px solid black; padding: 10px'>".$info['Agente']."</td><td style='border: 1px solid black; padding: 10px'>".$info['Llamante']."</td><td style='border: 1px solid black; padding: 10px'>".$info['Finalizada']."</td><td style='border: 1px solid black; padding: 10px'>".$info['Duracion']."</td></tr>";
    }

    echo "</tbody></table></body>";
  }

  public function rtCallsCO_get(){

    $pais = $this->uri->segment(3);

    $query = "SELECT 
                        COALESCE(descr_agente, a.Agent) AS agente,
                        NOMBREDEP(dep) AS Dep,
                        SUBSTR(Agent, 7, 100) AS Extension,
                        RT_caller AS caller,
                        COALESCE(q.shortName,RT_queue) as Q,
                        RT_queue as waitQ,
                        direction,
                        b.Pausa,
                        CASE 
                        WHEN RT_caller IS NOT NULL THEN 
			                    IF(RT_answered = 0,RT_entered,RT_answered)
                        WHEN Curpausecode != '' THEN 
                          IF(Freesincepauorcalltst > Curpausetst, Freesincepauorcalltst, Curpausetst)
                        ELSE IF(Freesincepauorcalltst != 0, Freesincepauorcalltst, Logon)
                        END as lastTst,
                        a.Queue,
                        RT_entered as waiting,
                        Curpausetst as origPauseTst,
                        RT_answered as answeredTst,
                        RT_dnis,
                        obCaller,
                        obTst,
                        pr.color,
                        a.Last_Update,
                        IF(a.Freesincepauorcalltst = 0, Logon, Freesincepauorcalltst) as freeSince
                    FROM
                        ccexporter.liveMonitor$pais a
                            LEFT JOIN
                        ccexporter.agentDetails nm ON a.Agent = nome_agente
                            LEFT JOIN
                        Tipos_pausas b ON Curpausecode = b.pausa_id
                            AND Curpausecode != ''
                            LEFT JOIN
                        dep_asesores dp ON nm.asesor = dp.asesor
                            AND dp.Fecha = CURDATE()
                            LEFT JOIN
                        Cola_Skill q ON RT_queue = q.queue
                            LEFT JOIN
                        PCRCs pr ON dp.dep = pr.id
                    ORDER BY RT_entered DESC";
    

    if( $rt = $this->db->query($query) ){
      okResponse("'Info Obtenida", "data", $rt->result_array(), $this);
    }else{
      errResponse("Error en base de datos",REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }

  public function getQs_get(){
    $query = "SELECT 
                  sede, b.id, Departamento, shortName as Cola, queue
              FROM
                  Cola_Skill a
                      LEFT JOIN
                  PCRCs b ON a.monShow = b.id
              WHERE
                  active = 1 
                  -- AND direction = 1
                  AND sede IS NOT NULL
              ORDER BY
              shortName";

    if( $q = $this->db->query($query) ){
      okResponse("'Info Obtenida", "data", $q->result_array(), $this);
    }else{
      errResponse("Error en base de datos",REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }
  }

  public function summaryRT_put(){

    $pais = $this->uri->segment(3);
    $data = $this->put();

    $this->db->select("SUM(IF(direction=1,calls,0)) AS ofrecidas,
                      SUM(IF(direction = 2 AND grupo!='abandon', calls, 0)) AS salientes,
                      SUM(IF(direction=1 AND grupo='abandon',calls,0)) AS abandonadas,
                      SUM(IF(direction=1 AND grupo!='abandon',sla20,0)) AS sla20,
                      SUM(IF(direction=1 AND grupo!='abandon',sla30,0)) AS sla30", FALSE)
              ->from('calls_summary a')
              ->join("PCRCs b", "a.skill = b.id", "left")
              ->where('Fecha = ', 'CURDATE()', FALSE)
              ->where('sede', $pais);
    
    if( count($data) > 0 ){
      $this->db->where_in('qNumber', $data);
    }

    if( $q = $this->db->get() ){
      okResponse("'Info Obtenida", "data", $q->row_array(), $this, 'count', count($data));
    }else{
      errResponse("Error en base de datos",REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }

  private function callStatsGenerator($params, $dt){
    if( $dt != 'forecast' ){
      $this->db->select("SUM(calls) as Offered")
              ->select("SUM(IF(grupo != 'abandon',COALESCE(calls,0),0)) as Answered")
              ->select("SUM(IF(grupo = 'abandon',COALESCE(calls,0),0)) as Abandoned")
              ->select("SUM(IF(grupo = 'pdv',COALESCE(calls,0),0)) as PDV")
              ->select("SUM(IF(grupo = 'apoyo',COALESCE(calls,0),0)) as apoyo")
              ->select("SUM(IF(grupo = 'main',COALESCE(calls,0),0)) as main")
              ->select("SUM(IF(grupo != 'abandon',tt,0))/SUM(IF(grupo != 'abandon',COALESCE(calls,0),0)) as AHT")
              ->select("SUM(IF(grupo = 'pdv',tt,0))/SUM(IF(grupo = 'pdv',COALESCE(calls,0),0)) as AHT_pdv")
              ->select("SUM(IF(grupo = 'apoyo',tt,0))/SUM(IF(grupo = 'apoyo',COALESCE(calls,0),0)) as AHT_apoyo")
              ->select("SUM(IF(grupo = 'main',tt,0))/SUM(IF(grupo = 'main',COALESCE(calls,0),0)) as AHT_main")
              ->select("SUM(IF(grupo != 'abandon',COALESCE(sla20,0),0)) as sla20")
              ->select("SUM(IF(grupo != 'abandon',COALESCE(sla30,0),0)) as sla30")
              ->select("SUM(ROUND(volumen*COALESCE(participacion,0))) as forecast")
              ->select("'$dt' as dt")
              ->from('calls_summary a')
              ->join('forecast_volume fv', 'a.Fecha = fv.Fecha AND fv.skill='.$params['skill'], 'left')
              ->join('forecast_participacion fp', 'a.Fecha = fp.Fecha AND fp.Hora = HOUR(a.Hora)*2+IF(Minute(a.Hora)=30,1,0) AND fp.skill='.$params['skill'], 'left', FALSE)
              ->where(array('a.skill' => $params['skill'], 'direction' => 1))
              ->order_by('H')
              ->group_by('H');
    }else{
      $this->db->select("b.Fecha,
                          Hora_time,
                          SUM(ROUND(volumen * participacion)) AS Offered", FALSE)
                ->from('HoraGroup_Table a')
                ->join('forecast_participacion b','a.Hora_int = b.hora','left')
                ->join('forecast_volume c','b.Fecha = c.Fecha AND b.skill = c.skill','left')
                ->where('b.skill', $params['skill'])
                ->order_by('H')
                ->group_by('H');
    }

            switch($dt){
              case 'forecast':
                $this->db->where('b.Fecha BETWEEN ', "'".$params['inicio']."' AND '".$params['fin']."'", FALSE);
                break;
              case 'td':
                $this->db->where('a.Fecha BETWEEN ', "'".$params['inicio']."' AND '".$params['fin']."'", FALSE);
                break;
              case 'lw':
                $this->db->where('a.Fecha BETWEEN ', "date_add('".$params['inicio']."', INTERVAL -7 DAY) AND date_add('".$params['fin']."', INTERVAL -7 DAY)", FALSE);
                break;
              case 'ly':
                $this->db->where('a.Fecha BETWEEN ', "date_add('".$params['inicio']."', INTERVAL -364 DAY) AND date_add('".$params['fin']."', INTERVAL -364 DAY)", FALSE);
                break;
            }

            switch( $params['groupBy'] ){
              case 'hora':
                if( $dt != 'forecast' ){
                  $this->db->select("CAST(CONCAT(a.Fecha,' ',CONCAT(HOUR(a.Hora),IF(MINUTE(a.Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                }else{
                  $this->db->select("CAST(CONCAT(b.Fecha,' ',CONCAT(HOUR(Hora_time),IF(MINUTE(Hora_time)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                }
                break;
              case 'dia':
                if( $dt != 'forecast' ){
                  $this->db->select("CAST(CONCAT(a.Fecha,' 00:00:00') as DATETIME) as H", FALSE);
                }else{
                  $this->db->select("CAST(CONCAT(b.Fecha,' 00:00:00') as DATETIME) as H", FALSE);
                }
                break;
              case 'mes':
                if( $dt != 'forecast' ){
                  $this->db->select("CAST(CONCAT(YEAR(a.Fecha),'-',MONTH(a.Fecha),'-01 00:00:00') as DATETIME) as H", FALSE);
                }else{
                  $this->db->select("CAST(CONCAT(YEAR(b.Fecha),'-',MONTH(b.Fecha),'-01 00:00:00') as DATETIME) as H", FALSE);
                }
                break;
              case 'inDay':
                if( $dt != 'forecast' ){
                  $this->db->select("CAST(CONCAT('$fin ',CONCAT(HOUR(a.Hora),IF(MINUTE(a.Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                }else{
                  $this->db->select("CAST(CONCAT('$fin ',CONCAT(HOUR(Hora_time),IF(MINUTE(Hora_time)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                }
                break;
            }
  }

  public function callStats_put(){

    $params = $this->put();

    //Totales
    $this->db->select("SUM(calls) as Offered")
            ->select("SUM(IF(grupo != 'abandon',calls,0)) as Answered")
            ->select("SUM(IF(grupo = 'abandon',calls,0)) as Abandoned")
            ->select("SUM(IF(grupo = 'pdv',calls,0)) as PDV")
            ->select("SUM(IF(grupo = 'apoyo',calls,0)) as apoyo")
            ->select("SUM(IF(grupo = 'main',calls,0)) as main")
            ->select("SUM(IF(grupo != 'abandon',sla20,0)) as sla20")
            ->select("SUM(IF(grupo != 'abandon',sla30,0)) as sla30")
            ->from('calls_summary')
            ->where('Fecha BETWEEN ', "'".$params['inicio']."' AND '".$params['fin']."'", FALSE)
            ->where(array('skill' => $params['skill'], 'direction' => 1));
            
    if( $tot = $this->db->get() ){

      $dates = array('td', 'lw', 'ly', 'forecast');
      $q = array();

      foreach( $dates as $index => $dt ){
        $this->callStatsGenerator( $params, $dt );
        if( $dt == 'ly' ){
          // okResponse('query', 'data', $this->db->get_compiled_select(), $this);
        }
        $q[$dt] = $this->db->get();
      }

      $result = array( 'forecast' => array(), 'td' => array(), 'ly' => array(), 'lw' => array(), 'total' => $tot->row_array() );

        foreach($q as $ind => $datos){
          foreach($datos->result_array() as $index => $info){
            array_push( $result[$ind], $info );
          }
        }

        $lq = $this->db->query("SELECT MAX(Last_Update) as lu FROM calls_summary WHERE Fecha='".$params['fin']."'");
        $lr = $lq->row_array();

        okResponse("Info Obtenida", "data", $result, $this, 'lu', $lr['lu']);

    }else{
      errResponse("Error en base de datos",REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }

  public function lostCalls_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $fecha = $this->uri->segment(3);
      $pais = $this->uri->segment(4);

      $query = "SELECT 
                    CAST(CONCAT(Fecha, ' ', Hora) as DATETIME) as Hora, Llamante, a.Cola, Espera
                FROM
                    t_Answered_Calls a
                        LEFT JOIN
                    Cola_Skill b ON a.qNumber = b.queue
                        LEFT JOIN
                    PCRCs p ON b.Skill = p.id
                WHERE
                    Fecha = '$fecha'
                        AND Answered = 0
                        AND direction = 1
                        AND sede = '$pais'
                ORDER BY Fecha DESC, Hora DESC";

    if( $q = $this->db->query($query) ){
      okResponse( 'Data obtenida', 'data', $q->result_array(), $this );
    }else{
      errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

    });

  }

  public function lastAhtLimit_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
      
      $query = "SELECT 
                    qNumber, SUM(tt) / SUM(calls) AS aht
                FROM
                    calls_summary
                WHERE
                    Fecha BETWEEN ADDDATE(CURDATE(), - 7) AND ADDDATE(CURDATE(), - 1)
                        AND grupo != 'abandon'
                GROUP BY qNumber";

      if( $q = $this->db->query($query) ){

        $result=array();

        foreach( $q->result_array() as $index => $aht ){
          $result[$aht['qNumber']] = $aht['aht'];
        }

        okResponse( 'Data obtenida', 'data', $result, $this );
      }else{
        errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }


}
