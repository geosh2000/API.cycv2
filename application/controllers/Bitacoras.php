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
      
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS calls");
      $this->db->query("CREATE TEMPORARY TABLE calls SELECT 
          a.*,
          HOUR(Hora) + IF(MINUTE(Hora) >= 30, .5, 0) AS HG,
          Skill,
          direction
      FROM
          t_Answered_Calls a
              LEFT JOIN
          Cola_Skill b ON a.Cola = b.Cola
      WHERE
          Fecha = @inicio
      HAVING direction = 1 AND Skill = @skill");
      $this->db->query("ALTER TABLE calls ADD PRIMARY KEY (Fecha, Hora, Llamante(15) )");
      $this->db->query("ALTER TABLE calls ADD INDEX skill (`Skill` ASC)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsSum");
      $this->db->query("CREATE TEMPORARY TABLE callsSum SELECT 
          HG, Skill, 
          COUNT(IF(Answered = 1
                  AND TIME_TO_SEC(Espera) <= IF(Skill IN (3 , 35), 20, 30),
              ac_id,
              NULL)) / COUNT(*) * 100 AS SLA,
          COUNT(*) AS Llamadas,
          COALESCE(AVG(IF(Answered = 1 AND dep!=29,
                      TIME_TO_SEC(Duracion_Real),
                      NULL)),
                  0) AS AHTDep,
          COALESCE(AVG(IF(Answered = 1 AND dep=29,
              TIME_TO_SEC(Duracion_Real),
              NULL)),
                  0) AS AHTPdv,
          COALESCE(AVG(IF(Answered = 1,
              TIME_TO_SEC(Duracion_Real),
              NULL)),
                  0) AS AHTTotal,
          COUNT(IF(Answered = 0, ac_id, NULL)) / COUNT(*) * 100 AS Abandon
      FROM
          calls a LEFT JOIN dep_asesores b ON a.asesor=b.asesor AND @inicio=b.Fecha
      GROUP BY HG, Skill");
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
          Hora_group
          ,dep
          ,COUNT(DISTINCT b.asesor) AS prog_normal
      FROM
          HoraGroup_Table a
              LEFT JOIN
          asesores_programacion b ON js <= CASTDATETIME(@inicio,CASTDATETIME(@inicio,Hora_end))
              AND je > CASTDATETIME(@inicio,Hora_time)
              LEFT JOIN
          dep_asesores dp ON b.asesor = dp.asesor
              AND @inicio = dp.Fecha 
              LEFT JOIN 
          asesores_ausentismos au ON b.asesor=au.asesor AND CAST(js as DATE)=au.Fecha 
              LEFT JOIN config_tiposAusentismos tp ON au.ausentismo=tp.id
      WHERE
          js BETWEEN ADDDATE(@inicio,-1) AND ADDDATE(@inicio,1) AND dep!=29 AND vacante IS NOT NULL AND js!=je AND js IS NOT NULL
          AND (COALESCE(au.a,0) = 0 OR tp.programable=0)
          AND dp.dep=@skill AND puesto != 11
      GROUP BY Hora_group , dep");
      $this->db->query("ALTER TABLE j ADD PRIMARY KEY (Hora_group , dep)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS x1");
      $this->db->query("CREATE TEMPORARY TABLE x1 SELECT 
          Hora_group,
          dep,
          COUNT(DISTINCT b.asesor) AS prog_x1
      FROM
          HoraGroup_Table a
              LEFT JOIN
          asesores_programacion b ON x1s <= CASTDATETIME(@inicio,Hora_end)
              AND x1e > CASTDATETIME(@inicio,Hora_time)
              LEFT JOIN
          dep_asesores dp ON b.asesor = dp.asesor
          AND @inicio = dp.Fecha 
              LEFT JOIN 
          asesores_ausentismos au ON b.asesor=au.asesor AND CAST(x1s as DATE)=au.Fecha 
              LEFT JOIN config_tiposAusentismos tp ON au.ausentismo=tp.id
      WHERE
          x1s BETWEEN ADDDATE(@inicio,-1) AND ADDDATE(@inicio,1) AND dep!=29 AND vacante IS NOT NULL AND x1s!=x1e AND x1s IS NOT NULL
          AND (COALESCE(au.a,0) = 0 OR tp.programable=0)
          AND dp.dep=@skill AND puesto != 11
      GROUP BY Hora_group , dep");
      $this->db->query("ALTER TABLE x1 ADD PRIMARY KEY (Hora_group , dep)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS x2");
      $this->db->query("CREATE TEMPORARY TABLE x2 SELECT 
          Hora_group,
          dep,
          COUNT(DISTINCT b.asesor) AS prog_x2
      FROM
          HoraGroup_Table a
              LEFT JOIN
          asesores_programacion b ON x2s <= CASTDATETIME(@inicio,Hora_end)
              AND x2e > CASTDATETIME(@inicio,Hora_time)
              LEFT JOIN
          dep_asesores dp ON b.asesor = dp.asesor
          AND @inicio = dp.Fecha 
              LEFT JOIN 
          asesores_ausentismos au ON b.asesor=au.asesor AND CAST(x2s as DATE)=au.Fecha 
              LEFT JOIN config_tiposAusentismos tp ON au.ausentismo=tp.id
      WHERE
          x2s BETWEEN ADDDATE(@inicio,-1) AND ADDDATE(@inicio,1) AND dep!=29 AND vacante IS NOT NULL AND x2s!=x2e AND x2s IS NOT NULL
          AND (COALESCE(au.a,0) = 0 OR tp.programable=0)
          AND dp.dep=@skill AND puesto != 11
      GROUP BY Hora_group , dep");
      $this->db->query("ALTER TABLE x2 ADD PRIMARY KEY (Hora_group , dep)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS prog");
      $this->db->query("CREATE TEMPORARY TABLE prog SELECT 
          j.Hora_group,
          j.dep,
          prog_normal AS j, 
          COALESCE(prog_x1, 0) + COALESCE(prog_x2, 0) AS x
      FROM
          j
              LEFT JOIN
          x1 ON j.Hora_group = x1.Hora_group
              AND j.dep = x1.dep
              LEFT JOIN
          x2 ON j.Hora_group = x2.Hora_group
              AND j.dep = x2.dep");
      $this->db->query("ALTER TABLE prog ADD PRIMARY KEY (Hora_group , dep)");

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
          pr.id as skill,
          COALESCE(j,0) as programados, COALESCE(x,0) as extra_programados,
          COALESCE(j,0)+COALESCE(x,0) as total_programados,
          COUNT(IF(dp.dep = skill, b.asesor, NULL)) AS racsDep,
          COUNT(DISTINCT b.asesor) AS racs
      FROM
          HoraGroup_Table a
              JOIN
              PCRCs pr LEFT JOIN 
          asist b ON a.Hora_group=b.Hora_group AND pr.id = b.Skill
              LEFT JOIN
          dep_asesores dp ON b.asesor = dp.asesor
              AND @inicio = dp.Fecha LEFT JOIN prog p ON a.Hora_group = p.Hora_group AND pr.id=p.dep
      WHERE pr.parent=1
      GROUP BY a.Hora_group , pr.id");
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
