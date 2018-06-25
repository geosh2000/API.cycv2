<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Nomina extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
    $this->load->model('Cliente_model');
  }

  public function cxcPendientes_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      if($q = $this->db->get_where('asesores_cxc', 'status = 1')){

        $result       = array(
                              'status'    => true,
                              'rows'      => $q->num_rows(),
                              'msg'       => "CxCs Pedientes Cargados"
                            );
      }else{
        $result       = array(
                              'status'    => false,
                              'rows'      => 0,
                              'data'      => null,
                              'msg'       => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function listCortes_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      if($q = $this->db->select("id, CONCAT(quincena, ' (', inicio, ' - ',fin,') | pago -> ',pago) as name")->order_by('inicio')->get_where('rrhh_calendarioNomina','inicio >= ADDDATE(CURDATE(),-145)')){

        $result       = array(
                              'status'    => true,
                              'rows'      => $q->num_rows(),
                              'data'      => $q->result_array(),
                              'msg'       => "Lista Nomina Carga"
                            );
      }else{
        $result       = array(
                              'status'    => false,
                              'rows'      => 0,
                              'data'      => null,
                              'msg'       => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function prenomina_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $nomina = $this->uri->segment(3);

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS datesNomina");
      $this->db->query("CREATE TEMPORARY TABLE datesNomina SELECT
                            Fecha, inicio AS inicioNomina, fin AS finNomina
                        FROM
                            Fechas a
                                RIGHT JOIN
                            rrhh_calendarioNomina b ON a.Fecha BETWEEN inicio AND fin
                        WHERE
                            id = $nomina");
      $this->db->query("SET @inicio = (SELECT MIN(Fecha) FROM datesNomina)");
      $this->db->query("SET @fin = (SELECT MAX(Fecha) FROM datesNomina)");
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS builtNomina");
      $this->db->query("CREATE TEMPORARY TABLE builtNomina SELECT
                          a.asesor as idAsesor,
                          num_colaborador AS CLAVE,
                          b.Nombre AS Nombre_del_empleado,
                          d.Ciudad AS Ubicacion,
                          NULL AS Centro_de_Costos,
                          g.nombre AS Unidad_de_Negocio,
                          f.nombre AS Area,
                          e.nombre AS Departamento,
                          h.nombre AS Puesto,
                          c.esquema as Esquema,
                          Ingreso,
                          IF(Egreso>='2030-01-01',NULL,Egreso) as Egreso,
                          ROUND(SALARIOASESOR(a.asesor, @fin, 'salario'),
                                  2) AS Salario
                      FROM
                          dep_asesores a
                              LEFT JOIN
                          Asesores b ON a.asesor = b.id
                              LEFT JOIN
                          asesores_plazas c ON a.vacante = c.id
                              LEFT JOIN
                          db_municipios d ON c.ciudad = d.id
                              LEFT JOIN
                          hc_codigos_Departamento e ON a.hc_dep = e.id
                              LEFT JOIN
                          hc_codigos_Areas f ON e.area = f.id
                              LEFT JOIN
                          hc_codigos_UnidadDeNegocio g ON f.unidadDeNegocio = g.id
                              LEFT JOIN
                          hc_codigos_Puesto h ON a.hc_puesto = h.id
                      WHERE
                          Fecha = @fin AND vacante IS NOT NULL
                      HAVING Area != 'PDV'");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS dateAsesorNomina");
      $this->db->query("CREATE TEMPORARY TABLE dateAsesorNomina SELECT
                        	Fecha, idAsesor
                        FROM
                        	datesNomina JOIN builtNomina");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS log_asesor");
      $this->db->query("CREATE TEMPORARY TABLE log_asesor (SELECT
                            a.*,
                            `jornada start`,
                            `jornada end`,
                            `comida start`,
                            `comida end`,
                            `extra1 start`,
                            `extra1 end`,
                            `extra2 start`,
                            `extra2 end`,
                            LOGASESOR(a.Fecha, a.idAsesor, 'in') AS login,
                            LOGASESOR(a.Fecha, a.idAsesor, 'out') AS logout
                        FROM
                            dateAsesorNomina a
                                LEFT JOIN
                            `Historial Programacion` b ON a.idAsesor = b.asesor AND a.Fecha = b.Fecha)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS xtra_time");
      $this->db->query("CREATE TEMPORARY TABLE xtra_time (SELECT Fecha as xtra_fecha, asesor as xtra_asesor,
                            TIME_TO_SEC(CAST(ADDTIME(
                            	IF(x1_start!=x1_logout,ADDTIME(CAST(if(x1_logout<'05:00:00',ADDTIME(x1_logout,'24:00:00'),x1_logout) as TIME),-CAST(if(x1_start<'05:00:00',ADDTIME(x1_start,'24:00:00'),x1_start) as TIME)),'00:00:00'),
                            	IF(x2_start!=x2_logout,ADDTIME(CAST(if(x2_logout<'05:00:00',ADDTIME(x2_logout,'24:00:00'),x2_logout) as TIME),-CAST(if(x2_start<'05:00:00',ADDTIME(x2_start,'24:00:00'),x2_start) as TIME)),'00:00:00')) as TIME))/60/60 as total
                            FROM
                            (SELECT
                            	a.Fecha, asesor, num_colaborador, Departamento, x1_inicio, x1_end, x2_login, x2_end, login, logout,
                            	IF(x1_inicio!=x1_end,IF(login<IF(x1_end<'05:00:00',ADDTIME(x1_end,'24:00:00'),x1_end) AND IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>=x1_inicio,IF(login<IF(x1_inicio<'05:00:00',ADDTIME(x1_inicio,'24:00:00'),x1_inicio),x1_inicio,login),NULL),NULL) as x1_start,
                            	IF(x1_inicio!=x1_end,
                            		IF(login<IF(x1_end<'05:00:00',ADDTIME(x1_end,'24:00:00'),x1_end) AND IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>=x1_inicio,
                            			IF(IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>IF(x1_end<'05:00:00',ADDTIME(x1_end,'24:00:00'),x1_end),
                            				x1_end,
                            				IF(logout>='24:00:00',ADDTIME(logout,'-24:00:00'),logout)),
                            			NULL),
                            		NULL) as x1_logout,

                            	IF(x2_login!=x2_end,IF(login<IF(x2_end<'05:00:00',ADDTIME(x2_end,'24:00:00'),x2_end) AND IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>=x2_login,IF(login<IF(x2_login<'05:00:00',ADDTIME(x2_login,'24:00:00'),x2_login),x2_login,login),NULL),NULL) as x2_start,
                            	IF(x2_login!=x2_end,IF(login<IF(x2_end<'05:00:00',ADDTIME(x2_end,'24:00:00'),x2_end) AND IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>=x2_login,IF(IF(logout<'05:00:00',ADDTIME(logout,'24:00:00'),logout)>IF(x2_end<'05:00:00',ADDTIME(x2_end,'24:00:00'),x2_end),x2_end,IF(logout>='24:00:00',ADDTIME(logout,'-24:00:00'),logout)),NULL),NULL) as x2_logout
                            FROM
                            	(
                            		SELECT Fecha, asesor, `extra1 start` as x1_inicio, `extra1 end` as x1_end, `extra2 start` as x2_login, `extra2 end` as x2_end
                            		FROM
                            			`Historial Programacion`
                            		WHERE
                            			Fecha BETWEEN @inicio AND @fin AND
                            			`extra1 start` IS NOT NULL AND
                            			`extra1 start`!=`extra1 end`
                            	) a
                            LEFT JOIN
                            	Asesores b
                            ON
                            	a.asesor=b.id
                            LEFT JOIN
                            	PCRCs c
                            ON
                            	b.`id Departamento`=c.id
                            LEFT JOIN
                            	log_asesor d
                            ON
                            	a.Fecha=d.Fecha AND
                            	a.asesor=d.idAsesor ) a
                            )");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS xtraNomina");
      $this->db->query("CREATE TEMPORARY TABLE xtraNomina SELECT xtra_asesor as asesor, SUM(total) as total FROM (SELECT * FROM xtra_time) a GROUP BY asesor");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaNomina");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaNomina SELECT
                      idAsesor as idAsesorAsistencia,
                      SUM(IF(Descanso AND Asistencia != 1 AND Code_Aus IS NULL,Descanso,0)) AS Descansos,
                      SUM(Asistencia AND Code_aus IS NULL AND Descanso!=1) AS Asistencias,
                      SUM(IF(Code_aus = 'CA', Ausentismo, 0)) AS Capacitacion,
                      SUM(IF(Code_aus = 'FJ', Ausentismo, 0)) AS F_Faltas_JUS,
                      SUM(IF(Code_aus = 'F', Ausentismo, 0)) + SUM(IF(Falta = 1 AND Code_aus IS NULL, Falta, 0)) AS F_Faltas_IN,
                      SUM(IF(Code_aus = 'SUS', Ausentismo, 0)) AS F_Suspension,
                      SUM(IF(Code_aus = 'INC_MT', Ausentismo, 0)) AS Maternidad,
                      SUM(IF(Code_aus = 'INC', Ausentismo, 0)) AS Enfermedad,
                      '' AS Accidente,
                      SUM(IF(Code_aus = 'INC_RT', Ausentismo, 0)) AS Acc_por_riesgo,
                      SUM(IF(Code_aus = 'PS', Ausentismo, 0)) AS F_Permiso_sin_g,
                      SUM(IF(Code_aus = 'PC', Ausentismo, 0)) AS F_Permiso_con_g,
                      SUM(IF(Code_aus = 'VAC', Ausentismo, 0)) AS F_Vacaciones,
                      SUM(IF((Asistencia = 1
                              AND (Descanso = 1 AND Code_aus IS NULL))
                              OR (Code_aus = 'DT' AND Asistencia = 1),
                          1,
                          0)) AS Descanso_Trabajado,
                      SUM(IF(Code_aus = 'FES', Ausentismo, 0)) AS Dia_Festivo,
                      SUM(IF(Asistencia = 1 AND Domingo = 1,
                          1,
                          0)) AS DomingosTrabajados
                  FROM
                      (SELECT
                          Fecha,
                              a.idAsesor,
                              IF(`jornada start` = `jornada end`, 1, 0) AS Descanso,
                              CASE
                                  WHEN login IS NULL THEN 0
                                  WHEN login IS NOT NULL THEN 1
                              END AS Asistencia,
                              CASE
                                  WHEN tipo_ausentismo IS NULL THEN 0
                                  ELSE 1
                              END AS Ausentismo,
                              CASE
                  				WHEN login IS NULL AND `jornada start` != `jornada end` THEN 1
                                  ELSE 0
                              END as Falta,
                              CASE
                                  WHEN
                                      tipo_ausentismo IS NOT NULL
                                  THEN
                                      CASE
                                          WHEN Descansos = 0 THEN c.Code
                                          WHEN
                                              Descansos != 0
                                          THEN
                                              CASE
                                                  WHEN DATEDIFF(fin, inicio) < 5 THEN IF(fin = Fecha OR inicio = Fecha, 'D', c.Code)
                                                  ELSE CASE
                                                      WHEN
                                                          esquema = 10
                                                      THEN
                                                          IF(WEEKDAY(Fecha) + 1 IN (6 , 7)
                                                              AND (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) < Descansos, 'D', c.Code)
                                                      ELSE IF(WEEKDAY(Fecha) + 1 = 7
                                                          AND (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) < Descansos, 'D', IF((FLOOR(((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) - (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) - FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) - (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) / 5)) / 5)) < Beneficios
                                                          AND WEEKDAY(Fecha) + 1 = 6, 'B', IF(Fecha = fin
                                                          AND Descansos - ((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) - (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) - FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) - (FLOOR((DAYOFYEAR(Fecha) - DAYOFYEAR(inicio)) / 7)) / 5)) > 0, 'D', c.Code)))
                                                  END
                                              END
                                      END
                              END AS Code_aus,
                              total AS horas_extra,
                              IF(WEEKDAY(Fecha) + 1 = 7, 1, 0) AS Domingo
                      FROM
                          log_asesor a
                      LEFT JOIN Ausentismos b ON a.idAsesor = b.asesor
                          AND Fecha BETWEEN inicio AND fin
                      LEFT JOIN `Tipos Ausentismos` c ON b.tipo_ausentismo = c.id
                      LEFT JOIN xtra_time d ON a.idAsesor = xtra_asesor
                          AND Fecha = xtra_fecha
                      LEFT JOIN builtNomina e ON a.idAsesor = e.idAsesor) a
                  GROUP BY idAsesor");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcNomina");
      $this->db->query("CREATE TEMPORARY TABLE cxcNomina SELECT
                            asesor,
                            ROUND(SUM(IF(tipo = 0, a.monto, 0)),2) AS cxcResponsabilidad,
                            ROUND(SUM(IF(tipo = 1, a.monto, 0)),2) AS cxcEmpleado
                        FROM
                            rrhh_pagoCxC a
                                LEFT JOIN
                            asesores_cxc b ON a.cxc = b.id
                        WHERE
                            a.activo = 1 AND a.cobrado = 0
                                AND a.quincena = $nomina
                        GROUP BY asesor");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS prenomina");
      $this->db->query("CREATE TEMPORARY TABLE prenomina SELECT
                            a.*,
                            NULL as TotalDias,
                            b.*,
                            ROUND(b.total*(Salario/30/8*2),2) as HorasExtra,
                            c.*,
                            ROUND(a.Salario/30*DomingosTrabajados*0.25,2) AS Prima_Dominical,
                            '' as Prima_Vacacional_1_SI, '' as Dias_de_prima_vac,
                            '' AS Subsidio_por_incapacidad,
                            '' AS Compensacion,
                            '' AS Incentivo,
                            '' AS Ayuda_de_renta,
                            '' AS Retroactivo,
                            '' AS Comedor,
                            '' AS Descuento_Celular,
                            '' AS Otras_Deducciones,
                            '' AS Curso_ingles,
                            '' AS Optica_otras_deducciones,
                            '' AS Servicio_dental,
                            '' AS aportacion_voluntariado,
                            cxcEmpleado AS Descuento_empleado,
                            cxcResponsabilidad AS Responsabilidad,
                            '' AS Tarjeta_vales,
                            '' AS Observaciones
                        FROM
                            builtNomina a
                                LEFT JOIN
                            xtraNomina b ON a.idAsesor = b.asesor
                                LEFT JOIN
                            asistenciaNomina c ON a.idAsesor = c.idAsesorAsistencia
                        		LEFT JOIN
                        	cxcNomina d ON a.idAsesor=d.asesor");

      if($q = $this->db->query("SELECT * FROM prenomina")){

        $result       = array(
                              'status'    => true,
                              'rows'      => $q->num_rows(),
                              'data'      => $q->result_array(),
                              'msg'       => "Prenomina Cargada"
                            );
      }else{
        $result       = array(
                              'status'    => false,
                              'rows'      => 0,
                              'data'      => null,
                              'msg'       => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );

  }

}
