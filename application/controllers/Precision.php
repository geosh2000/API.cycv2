<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Precision extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function getPrecision_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data['inicio'] = $this->uri->segment(3);
      $data['fin'] = $this->uri->segment(4);

      $this->detalleTelefonia( $data );
      $this->detalleBO( $data );

      $flag = true;

      if($tel = $this->db->query("SELECT Departamento, SUM(flag) / COUNT(*) AS prec
                                FROM precCalls GROUP BY skill HAVING prec!=0")){
                                  $telefonia = $tel->result_array();
                                }else{
                                  $flag=false;
                                  $error[]=$this->db->error();
                                }

      if($bo = $this->db->query("SELECT area as Departamento, SUM(flag) / COUNT(*) AS prec
                                FROM precBO GROUP BY Departamento HAVING prec!=0")){
                                  $boffice = $bo->result_array();
                                }else{
                                  $flag=false;
                                  $error[]=$this->db->error();
                                }

      if($flag){

        $datos = $telefonia;
        foreach($boffice as $index => $info){
          $datos[]=$info;
        }

        $result = array(
                        'status'  => true,
                        'data'    => $datos,
                        'msg'     => 'Resultados Obtenidos'
                      );
      }else{
        $result = array(
                        'status'  => false,
                        'data'    => null,
                        'msg'     => $error
                      );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function detalleTelefonia( $data ){

    $this->db->query("DROP TEMPORARY TABLE IF EXISTS precCalls");
    $this->db->query("CREATE TEMPORARY TABLE precCalls SELECT
                          a.Fecha,
                              a.skill,
                              Departamento,
                              calls / volumen * 100 AS prec,
                              IF(calls / volumen >= 0.85
                                  AND calls / volumen <= 1.15, 1, 0) AS flag
                      FROM
                          (SELECT
                          *
                      FROM
                          forecast_volume
                      WHERE
                          Fecha BETWEEN '".$data['inicio']."' AND '".$data['fin']."') a
                      LEFT JOIN (SELECT
                          Fecha, Skill, COUNT(*) AS calls
                      FROM
                          t_Answered_Calls a
                      LEFT JOIN Cola_Skill b ON a.Cola = b.Cola
                      WHERE
                          Fecha BETWEEN '".$data['inicio']."' AND '".$data['fin']."'
                      GROUP BY Fecha , Skill) b ON a.Fecha = b.Fecha AND a.skill = b.Skill
                      LEFT JOIN
                      PCRCs c ON a.skill = c.id");

  }

  public function detalleBO( $data ){

    $this->db->query("DROP TEMPORARY TABLE IF EXISTS precBO");
    $this->db->query("CREATE TEMPORARY TABLE precBO SELECT
                                    CAST(a.fecha_recepcion AS DATE) AS Fecha,
                                        b.area, bo_skill as id,
                                        COUNT(*) AS regs,
                                        c.volumen,
                                        IF(COUNT(*) / volumen >= 0.85
                                            AND COUNT(*) / volumen <= 1.15, 1, 0) AS flag
                                FROM
                                    bo_tipificacion a
                                LEFT JOIN bo_areas b ON a.area = b.bo_area_id
                                LEFT JOIN forecast_volume c ON CAST(a.fecha_recepcion AS DATE) = c.Fecha
                                    AND b.bo_skill = c.skill
                                WHERE
                                    CAST(a.fecha_recepcion AS DATE) BETWEEN '".$data['inicio']."' AND '".$data['fin']."'
                                        AND a.`status` != 8
                                GROUP BY c.Fecha , a.area");

  }

  public function getDetalle_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data['inicio'] = $this->uri->segment(3);
      $data['fin'] = $this->uri->segment(4);

      $this->detalleTelefonia( $data );
      $this->detalleBO( $data );

      $t = $this->db->get('precCalls');
      $b = $this->db->get('precBO');

      $result = array(
                          'status'  => true,
                          'data'    => array(
                                              'telefonia' => $t->result_array(),
                                              'bo'        => $b->result_array()
                                            )
                        );

      return $result;

    });

    jsonPrint( $result );

  }

  public function detalle_intervalo_get(){

    $this->load->helper('erlang');

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data['inicio'] = $this->uri->segment(3);
      $data['fin'] = $this->uri->segment(4);
      $data['skill'] = $this->uri->segment(5);

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS llamadas");
      $this->db->query("CREATE TEMPORARY TABLE llamadas SELECT
                            a.*, Skill
                        FROM
                            t_Answered_Calls a
                                LEFT JOIN
                            Cola_Skill b ON a.Cola = b.Cola
                        WHERE
                            Fecha BETWEEN '".$data['inicio']."' AND '".$data['fin']."'
                        HAVING Skill IN (".$data['skill'].")");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsPrec");
      $this->db->query("CREATE TEMPORARY TABLE callsPrec SELECT
                            Fecha,
                            HOUR(Hora)*2 + IF(MINUTE(HORA)<30,0,1) as HoraGroup,
                            Skill, COUNT(*) AS calls,
                            CASE
                    			WHEN Skill IN (3,35) THEN COUNT(IF(Answered=1 AND Espera<='00:00:20',ac_id,NULL))/COUNT(*)
                                ELSE COUNT(IF(Answered=1 AND Espera<='00:00:30',ac_id,NULL))/COUNT(*)
                            END as SLA
                        FROM
                            llamadas a
                        GROUP BY Fecha, HoraGroup, Skill
                        HAVING Skill IN (".$data['skill'].")");
      $this->db->query("ALTER TABLE callsPrec ADD PRIMARY KEY (`Fecha`, `HoraGroup`, `Skill`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS forecast");
      $this->db->query("CREATE TEMPORARY TABLE forecast SELECT
                            a.skill,
                            a.Fecha,
                            b.hora,
                            ROUND(volumen * participacion, 0) AS forecast,
                            a.AHT,
                            a.Reductores
                        FROM
                            forecast_volume a
                                LEFT JOIN
                            forecast_participacion b ON a.Fecha = b.Fecha AND a.skill = b.skill
                        WHERE
                            a.Fecha BETWEEN '".$data['inicio']."' AND '".$data['fin']."'");


      $this->db->query("DROP TEMPORARY TABLE IF EXISTS horariosPrec");
      $this->db->query("CREATE TEMPORARY TABLE horariosPrec
                        SELECT
                            a.Fecha, dep,
                            `jornada start` as js,
                            CASE
                        		WHEN `jornada start` = `jornada end` THEN '00:00:00'
                        		WHEN `jornada end` < '09:00:00' THEN ADDTIME(`jornada end`,'24:00:00')
                                ELSE `jornada end`
                        	END as je,
                            `extra1 start` as x1s,
                            CASE
                        		WHEN `extra1 start` = `extra1 end` THEN '00:00:00'
                        		WHEN `extra1 end` < '09:00:00' THEN ADDTIME(`extra1 end`,'24:00:00')
                                ELSE `extra1 end`
                        	END as x1e,
                            `extra2 start` as x2s,
                            CASE
                        		WHEN `extra2 start` = `extra2 end` THEN '00:00:00'
                        		WHEN `extra2 end` < '09:00:00' THEN ADDTIME(`extra2 end`,'24:00:00')
                                ELSE `extra2 end`
                        	END as x2e
                        FROM
                            `Historial Programacion` a
                                LEFT JOIN
                            dep_asesores b ON a.asesor = b.asesor
                                AND a.Fecha = b.Fecha
                        WHERE
                            a.Fecha BETWEEN '".$data['inicio']."' AND '".$data['fin']."'
                                AND dep IN (".$data['skill'].")
                                AND puesto = 1
                                AND vacante IS NOT NULL");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS progPrec");
      $this->db->query("CREATE TEMPORARY TABLE progPrec
                          SELECT
                              Fecha, Hora_int, dep AS skill, COUNT(*) AS Programados
                          FROM
                              HoraGroup_Table a
                                  LEFT JOIN
                              horariosPrec b ON ADDTIME(a.Hora_time, '00:15:00') BETWEEN js AND je
                                  OR ADDTIME(a.Hora_time, '00:15:00') BETWEEN x1s AND x1e
                                  OR ADDTIME(a.Hora_time, '00:15:00') BETWEEN x2s AND x2e
                          GROUP BY Fecha , Hora_int , skill
                          ORDER BY Fecha , Hora_int , skill");
      $this->db->query("ALTER TABLE progPrec ADD PRIMARY KEY (`Fecha`, `Hora_int`, `skill`)");

      $this->db->query("DROP TABLE IF EXISTS precisionPronosticoIntervalo");
      $this->db->query("CREATE TABLE precisionPronosticoIntervalo
                          SELECT
                              a.Fecha,
                              a.HoraGroup,
                              a.Skill,
                              forecast,
                              calls,
                              calls / forecast AS prec,
                              CASE
                                  WHEN calls / forecast BETWEEN 0.7 AND 1.3 THEN 1
                                  ELSE 0
                              END AS flag,
                              SLA,
                              IF(Programados IS NULL,0,Programados) as Programados,
                              Reductores,
                              AHT
                          FROM
                              callsPrec a
                                  LEFT JOIN
                              forecast b ON a.Fecha = b.Fecha
                                  AND a.HoraGroup = b.hora
                                  AND a.Skill = b.skill
                                  LEFT JOIN
                          	progPrec c ON a.Fecha=c.Fecha AND a.HoraGroup = c.Hora_int AND a.Skill=c.skill
                          ORDER BY Skill , Fecha , HoraGroup");

      $q = $this->db->query("SELECT * FROM precisionPronosticoIntervalo WHERE Skill IN (".$data['skill'].")");
      $table = $q -> result_array();

      foreach($table as $index => $info){
        if($info['forecast'] == 0){
          $erlang = 0;
          $requeridos = 0;
          $precision = 1;
        }else{
          $erlang = intval(agentno(	$info['forecast']/1800*$info['AHT'], 20,$info['AHT'],.8));
          $requeridos = intval($erlang/(1-$info['Reductores']));
          if($requeridos == 0){
            $precision = 1;
          }else{
            $precision = $info['Programados']/$requeridos;
          }

        }

        $result[$index] = $info;
        $result[$index]['erlang'] = $erlang;
        $result[$index]['requeridos'] = $requeridos;
        $result[$index]['CalidadProg'] = $precision;

      }

      if(isset($result)){
        $resultado = array(
                            'status'  => true,
                            'data'    => $result
                          );
      }else{
        $resultado = array(
                            'status'  => false,
                            'data'    => null,
                            'msg'     => array("msg" => "Error al recabar data")
                          );
      }

      return $resultado;


    });

    jsonPrint( $result );

  }

}
