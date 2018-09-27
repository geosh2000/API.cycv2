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
                  active = 1 AND direction = 1
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

    $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsRAW");

    $this->db->query("CREATE TEMPORARY TABLE callsRAW SELECT 
                          a.*, queue, direction, sede
                      FROM
                          t_Answered_Calls a
                              LEFT JOIN
                          Cola_Skill b ON a.Cola = b.Cola
                              LEFT JOIN
                          PCRCs pr ON b.Skill = pr.id
                      WHERE
                          Fecha = CURDATE()
                      HAVING direction = 1 AND sede='$pais'");

    $this->db->select("COUNT(IF(direction = 1, ac_id, NULL)) AS ofrecidas,
                        COUNT(IF(direction = 2 AND Answered=1, ac_id, NULL)) AS salientes,
                        COUNT(IF(Answered = 0 AND direction = 1, ac_id, NULL)) AS abandonadas,
                        COUNT(IF(Answered = 1 AND Espera<='00:00:20' AND direction = 1, ac_id, NULL)) AS sla20,
                        COUNT(IF(Answered = 1 AND Espera<='00:00:20' AND direction = 1, ac_id, NULL)) AS sla30", FALSE)
              ->from('callsRAW');
    
    if( count($data) > 0 ){
      $this->db->where_in('queue', $data);
    }

    if( $q = $this->db->get() ){
      okResponse("'Info Obtenida", "data", $q->row_array(), $this, 'count', count($data));
    }else{
      errResponse("Error en base de datos",REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }


}
