<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Monitoring extends REST_Controller {

  public function __construct(){

    parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->helper('base_venta');
        $this->load->helper('validators');
        $this->load->helper('jwt');
        $this->load->database();
  }

  public function horariosPdv_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $fecha = $this->uri->segment(3);
        $pais = $this->uri->segment(4);

        if(!isset($pais)){ $pais = 'MX'; }

        $this->db->query("SET @inicio='$fecha'");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS pdvVac");
        $this->db->query("CREATE TEMPORARY TABLE pdvVac
            SELECT 
                f.Fecha,
                a.id as pdvIdVac,
                customZone,
                COUNT(ap.id) AS plazas,
                COUNT(dp.asesor) AS cubiertos
            FROM
                Fechas f
                    JOIN
                PDVs a
                    LEFT JOIN
                cat_zones cz ON a.ciudad = cz.id
                    LEFT JOIN
                asesores_plazas ap ON a.id = ap.oficina AND ap.Status = 1
                    AND ap.fin >= LAST_DAY(@inicio)
                    AND ap.Activo = 1
                    LEFT JOIN
                dep_asesores dp ON ap.id = dp.vacante
                    AND dp.Fecha = f.Fecha
            WHERE
                pais = 'MX' AND a.Activo = 1
                    AND f.Fecha BETWEEN @inicio AND LAST_DAY(@inicio)
            GROUP BY Fecha , a.id");
        $this->db->query("ALTER TABLE pdvVac ADD PRIMARY KEY (Fecha, pdvIdVac)");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS pdvProgRev");
        $this->db->query("CREATE TEMPORARY TABLE pdvProgRev
            SELECT 
                pv.Fecha,
                pdvIdVac,
                customZone,
                NOMBREPDV(pdvIdVac, 1) AS PDV,
                FINDSUPERDAYPDV(@inicio, pdvIdVac, 2) AS Supervisor,
                FINDCOORDDAYPDV(@inicio, customZone, 2) AS Coordinador,
                plazas,
                cubiertos,
                COALESCE(descansos,0) as descansos,
                COALESCE(prog,0) as prog,
                CASE
                    WHEN COALESCE(prog,0)-COALESCE(descansos,0) > plazas THEN COALESCE(prog,0)-COALESCE(descansos,0)-plazas
                    WHEN COALESCE(prog,0)-COALESCE(descansos,0) = plazas THEN 0
                    WHEN COALESCE(prog,0)-COALESCE(descansos,0) < plazas THEN
                        CASE
                            WHEN COALESCE(prog,0)>COALESCE(descansos,0) THEN COALESCE(prog,0)-plazas
                            WHEN COALESCE(prog,0)=COALESCE(descansos,0) THEN IF(cubiertos=0,-1,cubiertos*-1)
                            WHEN COALESCE(prog,0)-COALESCE(descansos,0) > plazas THEN COALESCE(prog,0)-COALESCE(descansos,0)
                            WHEN COALESCE(prog,0)-COALESCE(descansos,0) <= (plazas-cubiertos) THEN 0
                            ELSE COALESCE(prog,0)-COALESCE(descansos,0)- cubiertos
                        END
                END as dif,
                -- IF(COALESCE(prog,0)-COALESCE(descansos,0)-plazas<0,COALESCE(prog,0)-COALESCE(descansos,0)-plazas+(plazas-cubiertos),COALESCE(prog,0)-COALESCE(descansos,0)-plazas<0) as dif,
                asesores, asesoresDescanso
            FROM
                pdvVac pv
                    LEFT JOIN
                (SELECT 
                a.Fecha,
                COALESCE(pdv, oficina) AS pdvOK,
                COUNT(*) AS prog,
                COUNT(IF(COALESCE(js, 0) = COALESCE(je, 0) OR au.id IS NOT NULL,
                    1,
                    NULL)) AS descansos,
                GROUP_CONCAT(IF(COALESCE(js, 0) = COALESCE(je, 0) OR au.id IS NOT NULL,
                    NOMBREASESOR(a.asesor, 2),
                    NULL)) AS asesoresDescanso,
                GROUP_CONCAT(IF(COALESCE(js, 0) = COALESCE(je, 0) OR au.id IS NOT NULL,
                    NULL,
                    NOMBREASESOR(a.asesor, 2))) AS asesores
            FROM
                asesores_programacion a
                    LEFT JOIN
                dep_asesores dp ON a.asesor = dp.asesor
                    AND a.Fecha = dp.Fecha
                    LEFT JOIN
                asesores_ausentismos au ON a.Fecha = au.Fecha
                    AND a.asesor = au.asesor
                    LEFT JOIN
                config_tiposAusentismos ta ON au.ausentismo = ta.id
            WHERE
                a.Fecha BETWEEN @inicio AND LAST_DAY(@inicio)
            GROUP BY a.Fecha , pdvOK) b ON pdvIdVac = pdvOK AND pv.Fecha = b.Fecha
            GROUP BY pv.Fecha , pdvIdVac");

        $byPdv = "SELECT 
            PDV,
            pdvIdVac as pdvId,
            COALESCE(FINDSUPERDAYPDV(@inicio, pdvIdVac, 2),'NA') AS Supervisor,
            COALESCE(FINDSUPERDAYPDV(@inicio, pdvIdVac, 3), 0) AS s_id,
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 2),'NA') AS Coordinador,
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 3),0) AS c_id,
            COUNT(IF(dif > 0, 1, NULL)) AS sobrePoblado,
            COUNT(IF(dif < 0, 1, NULL)) AS subPoblado,
            COUNT(IF(dif = 0, 1, NULL)) AS ok,
            COUNT(IF(dif = 0, 1, NULL)) / COUNT(*) as cumplimiento
        FROM
            pdvProgRev
        GROUP BY PDV";

        $bySuper = "SELECT 
            COALESCE(FINDSUPERDAYPDV(@inicio, pdvIdVac, 2),'NA') AS Supervisor,
            COALESCE(FINDSUPERDAYPDV(@inicio, pdvIdVac, 3), 0) AS s_id,
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 2),'NA') AS Coordinador,
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 3),0) AS c_id,
            COUNT(IF(dif > 0, 1, NULL)) AS sobrePoblado,
            COUNT(IF(dif < 0, 1, NULL)) AS subPoblado,
            COUNT(IF(dif = 0, 1, NULL)) AS ok,
            COUNT(IF(dif = 0, 1, NULL)) / COUNT(*) as cumplimiento
        FROM
            pdvProgRev
        GROUP BY Supervisor";

        $byCoord = "SELECT 
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 2),'NA') AS Coordinador,
            COALESCE(FINDCOORDDAYPDV(@inicio, customZone, 3),0) AS c_id,
            COUNT(IF(dif > 0, 1, NULL)) AS sobrePoblado,
            COUNT(IF(dif < 0, 1, NULL)) AS subPoblado,
            COUNT(IF(dif = 0, 1, NULL)) AS ok,
            COUNT(IF(dif = 0, 1, NULL)) / COUNT(*) as cumplimiento
        FROM
            pdvProgRev
        GROUP BY Coordinador";

        if( $ba = $this->db->query($byPdv) ){

            if( $bs = $this->db->query($bySuper) ){

                if( $bc = $this->db->query($byCoord) ){
                    if( $bDet = $this->db->query("SELECT * FROM pdvProgRev ORDER BY Fecha") ){
                        okResponse('Data Obtenida', 'data', array("byPdv" => $ba->result_array(), "bySuper" => $bs->result_array(), "byCoord" => $bc->result_array(), 'detalle' => $bDet->result_array()), $this);
                    }else{
                        errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                }else{
                    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }

            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }



}