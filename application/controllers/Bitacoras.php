<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Bitacoras extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();

  }

  public function bitacora_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $fecha = $this->uri->segment(3);
      $skill = $this->uri->segment(4);

      $this->db->query("SET @inicio = CAST('$fecha' as DATE)");
      $this->db->query("SET @skill = $skill");
      
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsSum");
      $this->db->query("CREATE TEMPORARY TABLE callsSum SELECT 
                HOUR(Hora) + IF(MINUTE(Hora) >= 30, .5, 0) AS HG,
                Skill,
                COALESCE(SUM(IF(@skill IN (35 , 3), sla20, sla30)) / SUM(calls) * 100,
                        100) AS SLA,
                COALESCE(SUM(calls), 0) AS Llamadas,
                COALESCE(SUM(IF(grupo = 'main', tt, 0)) / SUM(IF(grupo = 'main', calls, 0)),
                        0) AS AHTDep,
                COALESCE(SUM(IF(grupo = 'pdv', tt, 0)) / SUM(IF(grupo = 'pdv', calls, 0)),
                        0) AS AHTPdv,
                COALESCE(SUM(IF(grupo != 'abandon', tt, 0)) / SUM(IF(grupo != 'abandon', calls, 0)),
                        0) AS AHTTotal,
                COALESCE(SUM(IF(grupo = 'abandon', calls, 0)) / SUM(calls) * 100,
                        0) AS Abandon
            FROM
                calls_summary
            WHERE
                Fecha = @inicio AND Skill = @skill
                    AND direction = 1
            GROUP BY Hora;");
      $this->db->query("ALTER TABLE callsSum ADD PRIMARY KEY (HG, Skill)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS forecast");
      $this->db->query("CREATE TEMPORARY TABLE forecast SELECT 
          a.Fecha AS fecha_f,
          hora / 2 AS hg_f,
          a.skill AS skill_f,
          FLOOR(volumen * participacion) AS forecast, AHT
      FROM
          forecast_volume a
              LEFT JOIN
          forecast_participacion b ON a.Fecha = b.Fecha AND a.skill = b.skill
      WHERE
          a.Fecha = @inicio AND a.skill=@skill");
      $this->db->query("ALTER TABLE forecast ADD PRIMARY KEY (fecha_f, hg_f, skill_f )");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS j");
      $this->db->query("CREATE TEMPORARY TABLE j SELECT 
                            Hora_group,
                            dep,
                            COUNT(DISTINCT CASE
                                    WHEN
                                        js <= CASTDATETIME(@inicio, CASTDATETIME(@inicio, Hora_end))
                                            AND je > CASTDATETIME(@inicio, Hora_time)
                                    THEN
                                        b.asesor
                                END) AS prog_normal,
                            COUNT(DISTINCT CASE
                                    WHEN
                                        x1s <= CASTDATETIME(@inicio, CASTDATETIME(@inicio, Hora_end))
                                            AND x1e > CASTDATETIME(@inicio, Hora_time)
                                    THEN
                                        b.asesor
                                END) AS prog_x1,
                            COUNT(DISTINCT CASE
                                    WHEN
                                        x2s <= CASTDATETIME(@inicio, CASTDATETIME(@inicio, Hora_end))
                                            AND x2e > CASTDATETIME(@inicio, Hora_time)
                                    THEN
                                        b.asesor
                                END) AS prog_x2
                        FROM
                            HoraGroup_Table a
                                JOIN
                            (SELECT 
                                dp.Fecha, dp.asesor, dp.dep, js, je, x1s, x1e, x2s, x2e
                            FROM
                                dep_asesores dp
                            LEFT JOIN asesores_programacion p ON dp.asesor = p.asesor
                                AND dp.Fecha = p.Fecha
                            LEFT JOIN asesores_ausentismos au ON dp.asesor = au.asesor
                                AND CAST(js AS DATE) = au.Fecha
                            LEFT JOIN config_tiposAusentismos tp ON au.ausentismo = tp.id
                            WHERE
                                dp.dep = @skill AND dp.puesto != 11
                                    AND p.Fecha BETWEEN ADDDATE(@inicio, - 1) AND ADDDATE(@inicio, 1)
                                    AND COALESCE(js, 0) != je
                                    AND (COALESCE(au.a, 0) = 0
                                    OR tp.programable = 0)) b
                        GROUP BY Hora_group;");
      $this->db->query("ALTER TABLE j ADD PRIMARY KEY (Hora_group)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS prog");
      $this->db->query("CREATE TEMPORARY TABLE prog SELECT 
          Hora_group,
          prog_normal AS j, 
          COALESCE(prog_x1, 0) + COALESCE(prog_x2, 0) AS x
      FROM
          j");
      $this->db->query("ALTER TABLE prog ADD PRIMARY KEY (Hora_group)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asist");
      $this->db->query("CREATE TEMPORARY TABLE asist SELECT 
          asesor, Skill, Hora_group
      FROM
          asesores_logs b LEFT JOIN HoraGroup_Table a
          ON login <= CASTDATETIME(@inicio,Hora_end)
              AND logout >= CASTDATETIME(@inicio,Hora_time)
              AND TIMEDIFF(IF(logout > CASTDATETIME(@inicio,Hora_end),
                      CASTDATETIME(@inicio,Hora_end),
                      logout),
                  IF(login < CASTDATETIME(@inicio,Hora_time),
                      CASTDATETIME(@inicio,Hora_time),
                      login)) >= IF(NOW() BETWEEN CASTDATETIME(@inicio,Hora_time) AND CASTDATETIME(@inicio,Hora_end),
              SEC_TO_TIME(TIME_TO_SEC(TIMEDIFF(NOW(), CASTDATETIME(@inicio,Hora_time)) / 2)),
              '00:15:00')
      WHERE
          login BETWEEN ADDDATE(@inicio,-1) AND ADDDATE(@inicio, 1) AND asesor>0
          AND Skill = @skill
      HAVING Hora_group IS NOT NULL");
      // $this->db->query("ALTER TABLE asist ADD PRIMARY KEY (asesor, Skill, Hora_group)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS racs");
      $this->db->query("CREATE TEMPORARY TABLE racs SELECT
            a.Hora_group, Hora_time,
            IF(pr.id=@skill, @skill,-1000) as skill,
            IF(pr.id=@skill, @skill,-1000) as skillOK,
            COALESCE(j,0) as programados, COALESCE(x,0) as extra_programados,
            COALESCE(j,0)+COALESCE(x,0) as total_programados,
            COUNT(DISTINCT CASE WHEN dp.dep = skill THEN b.asesor END) AS racsDep,
            COUNT(DISTINCT b.asesor) AS racs
        FROM
            HoraGroup_Table a
                JOIN
                PCRCs pr LEFT JOIN 
            asist b ON a.Hora_group=b.Hora_group AND pr.id = b.Skill
                LEFT JOIN
            dep_asesores dp ON b.asesor = dp.asesor
                AND @inicio = dp.Fecha LEFT JOIN prog p ON a.Hora_group = p.Hora_group AND IF(pr.id=@skill, @skill,-1000)=@skill
        WHERE pr.parent=1
        GROUP BY a.Hora_group , skillOK");
      $this->db->query("ALTER TABLE racs ADD PRIMARY KEY (Skill, Hora_group)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS bitacora");
      $this->db->query("CREATE TEMPORARY TABLE bitacora SELECT 
          hg,
          skill,
          CONCAT('{',GROUP_CONCAT(CONCAT('\"',level,'\":{',
                '\"Fecha\":\"',
                Fecha,
                '\",\"HG\":',
                HG,
                ',\"skill\":',
                skill,
                ',\"accion\":',
                accion,
                ',\"level\":',
                level,
                ',\"asesorId\":',
                asesor,
                ',\"comments\":\"',
                REPLACE(comments,'\"', '\''),
                '\",\"asesor\":\"',
                NOMBREASESOR(asesor, 1),
                '\",\"last_update\":\"',
                last_update,
                '\"}')),'}') AS comments
      FROM
          bitacora_data
      WHERE
        Fecha=@inicio
      GROUP BY hg , skill");
      $this->db->query("ALTER TABLE bitacora ADD PRIMARY KEY (hg, skill)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS metasBit");
      $this->db->query("CREATE TEMPORARY TABLE metasBit SELECT 
          skill,
          CONCAT('{',
                  GROUP_CONCAT(CONCAT('\"',
                              tipo,
                              '\":{',
                              '\"meta\":',
                              meta,
                              ',\"secundaria\":',
                              COALESCE(secundaria, 0),
                              '}')),
                  '}') AS metas
      FROM
          metas_kpi
      WHERE
          MONTH(@inicio) = mes
              AND YEAR(@inicio) = anio
      GROUP BY skill");
      $this->db->query("ALTER TABLE metasBit ADD PRIMARY KEY (skill)");

      $query = "SELECT 
                    CAST(Hora_group as DECIMAL(4,1)) AS HG,
                    r.Skill,
                    NOMBREDEP(r.Skill) as Depto,
                    SLA,
                    forecast,
                    Llamadas,
                    Llamadas / forecast * 100 AS prec,
                    programados,
                    extra_programados,
                    total_programados,
                    IF(CASTDATETIME(@inicio, Hora_time)>NOW(),NULL,racsDep) as racsDep,
                    IF(CASTDATETIME(@inicio, Hora_time)>NOW(),NULL,racs) as racsTotal,
                    f.AHT AS AHT_pronostico,
                    a.AHTDep,
                    a.AHTPdv,
                    a.AHTTotal,
                    Abandon,
                    comments,
                    metas
                FROM
                    racs r
                        LEFT JOIN
                    bitacora bt ON r.Skill = bt.skill
                        AND Hora_group = bt.hg
                        LEFT JOIN
                    callsSum a ON a.HG = Hora_group AND a.Skill = r.Skill
                        LEFT JOIN
                    forecast f ON @inicio = fecha_f AND Hora_group = hg_f
                        AND r.Skill = skill_f
                        LEFT JOIN
                    metasBit mb ON r.Skill = mb.skill 
                WHERE
                    r.Skill != 0
                GROUP BY Hora_group , r.Skill
                HAVING r.Skill=@skill
                ORDER BY r.Skill , Hora_group";

      if( $result = $this->db->query($query) ){
        okResponse( 'Bitacora obtenida', 'data', $result->result_array(), $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }
    });
  }

  public function comments_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->select("*")
            ->select('NOMBREASESOR(asesor,1) as Nombre')
            ->from("bitacora_data")
            ->where($data);
      

      if( $result = $this->db->get() ){
        okResponse( 'Comentarios obtenidos', 'data', $result->row_array(), $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }
    });
  }

  public function new_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $insert = $this->db->set($data)
            ->set('asesor', $_GET['usid'])
            ->get_compiled_insert('bitacora_data');
    
        

      if( $this->db->query("$insert ON DUPLICATE KEY UPDATE comments=VALUES(comments), asesor=VALUES(asesor)") ){
        okResponse( 'Comentarios guardados', 'data', true, $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }
    });
  }

  public function delete_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->where($data);;
    
      if( $this->db->delete('bitacora_data') ){
        okResponse( 'Comentarios borados', 'data', true, $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }
    });
  }

  public function actions_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('*')
            ->from('bitacora_acciones')
            ->where('activo',1)
            ->order_by('Actividad');
    
      if( $q = $this->db->get() ){
        okResponse( 'Acciones Obtenidas', 'data', $q->result_array(), $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }
    });
  }

  public function addEntry_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $insert = array(
                      'asesor' => $data['asesor'],
                      'actividades' => nl2br($data['comments'])
                    );

      if($this->db->set($insert)
                  ->set('date_created', 'NOW()', FALSE)
                  ->insert('bitacoras_supervisores')){
                    $result = array('status' => true, 'msg' => 'Guardado correctamente');
                  }else{
                    $result = array('status' => false, 'msg' => $this->db->error());
                  }

      return $result;

    });

    jsonPrint( $result );

  }

}
