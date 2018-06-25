<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
date_default_timezone_set('America/Mexico_city');
// use REST_Controller;


class Tablaf extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function mp_get(){
    
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        // =================================================
        // START Params
        // =================================================
            $inicio = $this->uri->segment(3);
            $fin = $this->uri->segment(4);
            $skill = $this->uri->segment(5);
            
            // Incluye o excluye el canal de agencias
            if($skill == 7){
                $agencias = "";
            }else{
                $agencias = "AND gpoCanalKpi!='Agencias'";
            }
            
            // Incluye a todos los paises para MT
            if($skill == 3){
                $pais = "";
            }else{
                $pais = "AND pais != 'CO'";
            }

            // Definición de parámetros por skill
            $params = array(
                '3'     => array( 'skin' => '3', 'skout' => '52', 'skill' => "(3,52)", 'marca' => "'Marcas Terceros'" ),
                '7'     => array( 'skin' => '7', 'skout' => '7', 'skill' => "(7)", 'marca' => "'Marcas Terceros'" ),
                '8'     => array( 'skin' => '8', 'skout' => '8', 'skill' => "(8)", 'marca' => "'Marcas Terceros'" ),
                '9'     => array( 'skin' => '9', 'skout' => '9', 'skill' => "(9)", 'marca' => "'Marcas Terceros'" ),
                '4'     => array( 'skin' => '4', 'skout' => '4', 'skill' => "(4)", 'marca' => "'Marcas Terceros'" ),
                '35'    => array( 'skin' => '35', 'skout' => '5', 'skill' => "(35,5,50)", 'marca' => "'Marcas Propias'" )
            );
        // =================================================
        // END Params
        // =================================================

        // =================================================
        // START Definición de variables para query
        // =================================================
            $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
            $this->db->query("SET @fin = CAST('$fin' as DATE)");
            $this->db->query("SET @marca = ".$params[$skill]['marca']);
        // =================================================
        // END Definición de variables para query
        // =================================================

        // =================================================
        // START Query Locs
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");
            $this->db->query("CREATE TEMPORARY TABLE locs
                                SELECT 
                                        a.Fecha,
                                        Localizador,
                                        gpoCanalKpi,
                                        cc,
                                        a.asesor as asesorLoc,
                                        tipoRsva,
                                        SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                                        IF(SUM(VentaMXN) > 0, Localizador, NULL) AS NewLoc,
                                        IF(tipoRsva LIKE '%OUT%' AND SUM(VentaMXN) > 0
                                                AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                            Localizador,
                                            NULL) AS CountLocOut,
                                        IF(tipoRsva LIKE '%IN' AND SUM(VentaMXN) > 0
                                                AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                            Localizador,
                                            NULL) AS CountLocIn,
                                        IF(SUM(VentaMXN) >= 0
                                                OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                            Localizador,
                                            NULL) AS ModifLoc
                                    FROM
                                        t_Locs a
                                            LEFT JOIN
                                        chanGroups b ON a.chanId = b.id
                                            LEFT JOIN
                                        dep_asesores c ON a.asesor = c.asesor
                                            AND a.Fecha = c.Fecha
                                            LEFT JOIN cc_apoyo e ON a.asesor=e.asesor AND a.Fecha BETWEEN e.inicio AND e.fin
                                            LEFT JOIN
                                        config_tipoRsva d ON IF(c.dep IS NULL,
                                            IF(a.asesor = - 1, - 1, 0),
                                            IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                0,
                                                IF(c.dep = 29 AND cc IS NOT NULL, 35, c.dep))) = d.dep
                                            AND IF(a.tipo IS NULL OR a.tipo = '',
                                            0,
                                            a.tipo) = d.tipo 
                                    WHERE
                                        a.Fecha BETWEEN @inicio AND @fin
                                            AND marca = @marca
                                            AND gpoCanalKpi != 'Outlet'
                                            $pais 
                                            $agencias
                                    GROUP BY Fecha , Localizador;");
        // =================================================
        // END Query Locs
        // =================================================

        // =================================================
        // START Query KPIs Venta
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS kpisVenta");
            $this->db->query("CREATE TEMPORARY TABLE kpisVenta
                SELECT 
                    a.Fecha,
                    CASE 
                        WHEN asesorLoc = 0 AND 3 != ".$params[$skill]['skin']." THEN 0
                        WHEN tipoRsva LIKE '%Tag%' THEN 50 
                        WHEN tipoRsva LIKE '%Out' THEN ".$params[$skill]['skout']."
                        WHEN tipoRsva LIKE '%IN' THEN ".$params[$skill]['skin']."
                        WHEN tipoRsva LIKE '%Presencial%' THEN 29 ELSE 0 END as Skill,
                    CASE
                        WHEN asesorLoc = 0 THEN 'Otros'
                        WHEN tipoRsva LIKE '%PDV%' THEN
                            CASE 
                                WHEN cc IS NULL THEN 'PDV'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        WHEN tipoRsva LIKE '%Presencial%' THEN 'Presencial'
                        WHEN tipoRsva LIKE '%online%' THEN 'Online'
                        WHEN tipoRsva LIKE '%out%' THEN
                            CASE
                                WHEN tipoRsva LIKE 'out' THEN 'CC'
                                WHEN tipoRsva LIKE '%Tag%' THEN 'CC'
                                ELSE 'Otros'
                            END
                        WHEN tipoRsva LIKE '%IN' THEN
                            CASE 
                                WHEN cc IS NULL THEN 'CC'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        ELSE 'CC'
                    END as Grupo,
                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS monto,
                    SUM(IF(servicio = 'Hotel',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_hotel,
                    SUM(IF(servicio = 'Tour',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_tour,
                    SUM(IF(servicio = 'Transfer',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_transfer,   
                    SUM(clientNights) AS RN,
                    SUM(costo) AS margen,
                    COUNT(DISTINCT CountLocOut) AS LocsOut,
                    COUNT(DISTINCT CountLocIn) AS LocsIn
                FROM
                    t_hoteles_test a
                        RIGHT JOIN
                    locs b ON a.Localizador = b.Localizador
                        AND a.Fecha = b.Fecha
                        LEFT JOIN
                    itemTypes c ON itemType = c.type
                        AND categoryId = c.category
                        LEFT JOIN
                    t_margen d ON a.Localizador = d.Localizador
                        AND a.item = d.Item
                        AND a.Fecha = d.Fecha
                GROUP BY a.Fecha , Skill, Grupo");
        // =================================================
        // END Query KPIs Venta
        // =================================================

        // =================================================
        // START Query Calls
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS calls");
            $this->db->query("CREATE TEMPORARY TABLE calls SELECT 
                    a.*, CASE
                        WHEN a.asesor=0 THEN 'Otros'
                        WHEN dep=29 AND cc IS NULL THEN 'PDV'
                        WHEN dep=29 AND cc IS NOT NULL THEN 'Apoyo'
                        WHEN Skill = 5 THEN 
                        CASE 
                            WHEN dep=5 THEN 'CC'
                            ELSE 'Otros'
                        END
                        ELSE 'CC'
                    END as grupo,
                    Skill, direction
                FROM
                    t_Answered_Calls a
                        LEFT JOIN
                    Cola_Skill b ON a.Cola = b.Cola
                        LEFT JOIN
                    dep_asesores c ON a.asesor = c.asesor
                        AND a.Fecha = c.Fecha
                        LEFT JOIN
                    cc_apoyo d ON a.asesor = d.asesor
                        AND a.Fecha BETWEEN d.inicio AND d.fin
                WHERE
                    a.Fecha BETWEEN @inicio AND @fin
                HAVING Skill IN ".$params[$skill]['skill']);
        // =================================================
        // END Query Calls
        // =================================================

        // =================================================
        // START Query Logueo Asesores
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS logAsesores");
            $this->db->query("CREATE TEMPORARY TABLE logAsesores SELECT 
                                a.*, IF(skill = dep, a.asesor, NULL) AS DistAsesor, dep, cc
                            FROM
                                asesores_logs a
                                    LEFT JOIN
                                dep_asesores b ON a.asesor = b.asesor
                                    AND CAST(login AS DATE) = b.Fecha
                                    LEFT JOIN
                                cc_apoyo d ON a.asesor = d.asesor
                                    AND CAST(login AS DATE) BETWEEN d.inicio AND d.fin
                            WHERE
                                login BETWEEN @inicio AND CAST(CONCAT(@fin, ' 23:59:00') AS DATETIME)
                                    AND skill IN ".$params[$skill]['skill']);

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS logsOK");
            $this->db->query("CREATE TEMPORARY TABLE logsOK SELECT 
                                CAST(login AS DATE) AS FechaOK,
                                skill,
                                CASE
                                    WHEN asesor=0 THEN 'Otros'
                                    WHEN dep=29 AND cc IS NULL THEN 'PDV'
                                    WHEN dep=29 AND cc IS NOT NULL THEN 'Apoyo'
                                    WHEN skill = 5 THEN 
                                    CASE 
                                        WHEN dep=5 THEN 'CC'
                                        ELSE 'Otros'
                                    END
                                    ELSE 'CC'
                                END as grupo,
                                SUM(TIME_TO_SEC(Duracion)) AS Sesion,
                                SUM(TIME_TO_SEC(Duracion))/28800 AS Ftes,
                                COUNT(DISTINCT DistAsesor) as HC_dia
                            FROM
                                logAsesores
                            GROUP BY FechaOK , skill, grupo");
        // =================================================
        // END Query Logueo Asesores
        // =================================================

        // =================================================
        // START Query Pausas
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS pauseAsesores");
            $this->db->query("CREATE TEMPORARY TABLE pauseAsesores SELECT 
                                    a.*, Productiva, IF(skill = dep, a.asesor, NULL) AS DistAsesor, dep, cc
                                FROM
                                    asesores_pausas a
                                        LEFT JOIN
                                    dep_asesores b ON a.asesor = b.asesor
                                        AND CAST(a.Inicio AS DATE) = b.Fecha LEFT JOIN Tipos_pausas c ON c.pausa_id = a.tipo 
                                        LEFT JOIN
                                    cc_apoyo d ON a.asesor = d.asesor
                                        AND CAST(a.Inicio AS DATE) BETWEEN d.inicio AND d.fin
                                WHERE
                                    a.Inicio BETWEEN @inicio AND CAST(CONCAT(@fin, ' 23:59:00') AS DATETIME)
                                        AND skill IN ".$params[$skill]['skill']);

                                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pauseOK");
                                $this->db->query("CREATE TEMPORARY TABLE pauseOK SELECT 
                                    CAST(a.Inicio AS DATE) AS FechaOK,
                                    skill,
                                    CASE
                                        WHEN asesor=0 THEN 'Otros'
                                        WHEN dep=29 AND cc IS NULL THEN 'PDV'
                                        WHEN dep=29 AND cc IS NOT NULL THEN 'Apoyo'
                                        WHEN skill = 5 THEN 
                                        CASE 
                                            WHEN dep=5 THEN 'CC'
                                            ELSE 'Otros'
                                        END
                                        ELSE 'CC'
                                    END as grupo,
                                    SUM(IF(Productiva = 0,
                                        TIME_TO_SEC(Duracion),
                                        0)) AS PNP,
                                    SUM(IF(Productiva = 1,
                                        TIME_TO_SEC(Duracion),
                                        0)) AS PP
                                FROM
                                    pauseAsesores a
                                GROUP BY FechaOK , skill, grupo");
        // =================================================
        // END Query Pausas
        // =================================================
        
        // =================================================
        // START Query Consolidación Log y Pausa
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS sesionesOK");
            $this->db->query("CREATE TEMPORARY TABLE sesionesOK SELECT 
                a.*,
                COALESCE(PNP,0) as PNP,
                COALESCE(PP,0) as PP,
                Sesion - COALESCE(PNP,0) AS Utilizacion
            FROM
                logsOK a
                    LEFT JOIN
                pauseOK b ON a.FechaOK = b.FechaOK
                    AND a.skill = b.skill AND a.grupo=b.grupo");
        // =================================================
        // END Query Consolidación Log y Pausa
        // =================================================

        // =================================================
        // START Query KPIs Telefónicos
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS kpisTel");
            $this->db->query("CREATE TEMPORARY TABLE kpisTel
                    SELECT 
                        a.Fecha,
                        a.Skill,
                        a.grupo,
                        COUNT(IF(direction = 1, ac_id, NULL)) AS inOfrecidas,
                        COUNT(IF(direction = 1 AND Desconexion != 'Abandono',
                            ac_id,
                            NULL)) AS inContestadas,
                        COUNT(IF(direction = 1 AND Desconexion = 'Abandono',
                            ac_id,
                            NULL)) AS inAbandonadas,
                        COUNT(IF(direction = 1 AND Desconexion != 'Abandono'
                                AND TIME_TO_SEC(Espera) <= 20,
                            ac_id,
                            NULL)) AS inSLA,
                        SUM(IF(direction = 1 AND Desconexion != 'Abandono',
                            TIME_TO_SEC(Duracion_Real),
                            0)) AS inTT,
                        SUM(IF(direction = 1 AND Desconexion != 'Abandono',
                            TIME_TO_SEC(Espera),
                            0)) AS inWait,
                        COUNT(IF(direction = 1 AND Desconexion != 'Abandono'
                                AND Desconexion = 'Transferida'
                                AND TIME_TO_SEC(Duracion_Real) < 120,
                            ac_id,
                            NULL)) AS inXfered,
                        COUNT(IF(direction = 2 AND Desconexion != 'Abandono',
                            ac_id,
                            NULL)) AS outEfectivas,
                        SUM(IF(direction = 2 AND Desconexion != 'Abandono',
                            TIME_TO_SEC(Duracion_Real),
                            0)) AS outEfectivasTT,
                        COUNT(IF(direction = 2 AND Desconexion = 'Abandono',
                            ac_id,
                            NULL)) AS outIntentos,
                        SUM(IF(direction = 2 AND Desconexion = 'Abandono',
                            TIME_TO_SEC(Espera),
                            0)) AS outIntentosTT,
                        Sesion, PNP, PP, Utilizacion,
                        Ftes, HC_dia
                    FROM
                        calls a
                            LEFT JOIN
                        sesionesOK d ON a.Fecha = d.FechaOK
                            AND a.skill = d.skill AND a.grupo=d.grupo
                    GROUP BY a.Fecha , a.Skill, grupo;");
        // =================================================
        // END Query KPIs Telefónicos
        // =================================================
        
        // =================================================
        // START Query Resultados
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS result");
            $this->db->query("CREATE TEMPORARY TABLE result
                                SELECT 
                                    b.Fecha, b.Skill, b.grupo, IF(b.Skill=0, 'Online', NOMBREDEP(b.Skill)) as Dep,
                                    COALESCE(monto,0) as monto, 
                                    COALESCE(monto_hotel,0) as monto_hotel, 
                                    COALESCE(monto_tour,0) as monto_tour, 
                                    COALESCE(monto_transfer,0) as monto_transfer, 
                                    COALESCE(monto,0)-COALESCE(margen,0) as margen,
                                    COALESCE(RN,0) as RN, 
                                    COALESCE(LocsIn,0) as LocsIn, 
                                    COALESCE(LocsOut,0) as LocsOut, 
                                    COALESCE(inOfrecidas,0) as inOfrecidas,
                                    COALESCE(inAbandonadas,0) as inAbandonadas,
                                    COALESCE(inAbandonadas,0)/COALESCE(inOfrecidas,0) as pAbandon,
                                    COALESCE(inSLA,0) as inSLA, 
                                    IF(COALESCE(inOfrecidas,0) = 0, 0, COALESCE(inSLA,0)/COALESCE(inOfrecidas,0)) as pSLA, 
                                    COALESCE(inWait,0) as inWait,
                                    COALESCE(inContestadas,0) as inContestadas,
                                    COALESCE(inTT,0) as inTT, 
                                    COALESCE(inTT,0)/COALESCE(inContestadas,0) as inAHT, 
                                    COALESCE(inXfered,0) as inXfered,
                                    IF(COALESCE(inContestadas,0)-COALESCE(inXfered,0) = 0, 0, COALESCE(LocsIn,0)/(COALESCE(inContestadas,0)-COALESCE(inXfered,0))) as FC, 
                                    COALESCE(outEfectivas,0) as outEfectivas, 
                                    COALESCE(outEfectivasTT,0) as outEfectivasTT,
                                    COALESCE(outEfectivasTT,0)/ COALESCE(outEfectivas,0) as outEfectivasAHT,
                                    COALESCE(outIntentos,0) as outIntentos, 
                                    COALESCE(outIntentosTT,0) as outIntentosTT, 
                                    COALESCE(Sesion,0) as Sesion, 
                                    COALESCE(PNP,0) as PNP, 
                                    COALESCE(PP,0) as PP,
                                    COALESCE(Utilizacion,0) as Utilizacion, 
                                    COALESCE(Utilizacion,0)/COALESCE(Sesion,0) as pUtilizacion, 
                                    COALESCE(inTT,0) + COALESCE(outEfectivasTT,0) + COALESCE(outIntentosTT,0) as Ocupacion, 
                                    (COALESCE(inTT,0) + COALESCE(outEfectivasTT,0) + COALESCE(outIntentosTT,0))/COALESCE(Utilizacion,0) as pOcupacion, 
                                    COALESCE(Ftes,0) as Ftes, 
                                    COALESCE(HC_dia,0) as HC_dia
                                FROM
                                    kpisVenta a
                                        RIGHT JOIN
                                    kpisTel b ON a.Fecha = b.Fecha AND a.Skill = b.Skill AND a.grupo=b.grupo");
        // =================================================
        // END Query Resultados
        // =================================================

        // =================================================
        // START BUILD JSON FOR API
        // =================================================

            if( $q = $this->db->query("SELECT * FROM result") ){
                
                // =================================================
                // START BASE QUERY
                // =================================================
                    $query = "SELECT
                                Fecha, Skill, grupo, IF(Skill=0, 'Online', NOMBREDEP(Skill)) as Dep,
                                SUM(monto) as monto, 
                                SUM(monto_hotel) as monto_hotel, 
                                SUM(monto_tour) as monto_tour, 
                                SUM(monto_transfer) as monto_transfer, 
                                SUM(margen) as margen, 
                                SUM(RN) as RN, 
                                SUM(LocsIn) as LocsIn, 
                                SUM(LocsOut) as LocsOut, 
                                SUM(inOfrecidas) as inOfrecidas, 
                                SUM(inAbandonadas) as inAbandonadas, 
                                SUM(inAbandonadas)/SUM(inOfrecidas) as pAbandon, 
                                SUM(inSLA) as inSLA, 
                                IF(SUM(inOfrecidas) = 0,0,SUM(inSLA)/SUM(inOfrecidas)) as pSLA, 
                                SUM(inWait) as inWait, 
                                SUM(inContestadas) as inContestadas, 
                                SUM(inTT) as inTT, 
                                SUM(inTT)/SUM(inContestadas) as inAHT, 
                                SUM(inXfered) as inXfered, 
                                SUM(LocsIn)/SUM(inContestadas) as FC, 
                                SUM(outEfectivas) as outEfectivas, 
                                SUM(outEfectivasTT) as outEfectivasTT, 
                                SUM(outEfectivasTT)/SUM(outEfectivas) as outEfectivasAHT, 
                                SUM(outIntentos) as outIntentos, 
                                SUM(outIntentosTT) as outIntentosTT, 
                                SUM(Sesion) as Sesion, 
                                SUM(PNP) as PNP, 
                                SUM(PP) as PP, 
                                SUM(Utilizacion) as Utilizacion, 
                                SUM(Utilizacion)/SUM(Sesion) as pUtilizacion, 
                                SUM(Ocupacion) as Ocupacion, 
                                SUM(Ocupacion)/SUM(Utilizacion) as pOcupacion, 
                                SUM(Ftes) as Ftes, 
                                SUM(HC_dia) as HC_dia
                            FROM
                            result GROUP BY";
                // =================================================
                // END BASE QUERY
                // =================================================
                

                $td = $this->db->query("$query Fecha, Skill");
                $tg = $this->db->query(str_replace('SUM(Ftes)','SUM(Ftes)/COUNT(DISTINCT Fecha)',str_replace('SUM(HC_dia)','SUM(HC_dia)/COUNT(DISTINCT Fecha)',$query))." Skill, grupo");
                $ts = $this->db->query(str_replace('SUM(Ftes)','SUM(Ftes)/COUNT(DISTINCT Fecha)',str_replace('SUM(HC_dia)','SUM(HC_dia)/COUNT(DISTINCT Fecha)',$query))." Skill");
                    
                    foreach( $q->result_array() as $index => $info ){
                        $result[$info['Dep']][$info['grupo']][$info['Fecha']] = $info;
                    }
                    
                    foreach( $td->result_array() as $index => $info ){
                        $result[$info['Dep']]['all'][$info['Fecha']] = $info;
                    }
                
                    foreach( $tg->result_array() as $index => $info ){
                        $result[$info['Dep']][$info['grupo']]['Total'] = $info;
                    }
                
                    foreach( $ts->result_array() as $index => $info ){
                        $result[$info['Dep']]['all']['Total'] = $info;
                    }
                    
                    okResponse('Información Obtenida', 'data', $result, $this);
                
            }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
        // =================================================
        // END BUILD JSON FOR API
        // =================================================

        return true;
    });
      
  }

}
