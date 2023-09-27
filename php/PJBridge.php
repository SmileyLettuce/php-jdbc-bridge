<?php

namespace SmileyLettuce\JDBCBridge;

use Exception;
use Throwable;
use JsonException;

/*
 * Copyright (C) 2007 lenny@mondogrigio.cjb.net
 *
 * This file is part of PJBS (http://sourceforge.net/projects/pjbs)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

class PJBridge
{
    private $sock;

    /**
     * @param string $host
     * @param int $port
     * @throws Exception
     */
    public function __construct(string $host="localhost", int $port=4444)
    {

        $this->sock  = fsockopen($host, $port, $error_code, $error_message);

        if (!$this->sock) {
            throw new Exception($error_code . ": " . $error_message);
        }
    }

    /**
     *
     */
    public function __destruct()
    {
        fclose($this->sock);
    }


    /**
     * @return array<mixed>
     * @throws Exception
     */
    private function parse_reply(): array
    {

        $returnArr = [];
        $ret = [];

        $returnString = fgets($this->sock);

        if($returnString === false) {
            throw new Exception('Unable to get ouput from socket');
        }

        try {
            $returnArr = json_decode($returnString, true, flags: JSON_BIGINT_AS_STRING | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new Exception('Unable to decode contents');
        }

        if(is_array($returnArr) === false) {
            throw new Exception('Returned message not an array');
        }

        if(!isset($returnArr['status'])) {
            throw new Exception('Unable to get status');
        } else {
            $ret['status'] = $returnArr['status'];
        }

        if(array_key_exists('status', $returnArr) && $returnArr['status'] === "error") {
            throw new Exception($returnArr['message']);
        }

        if(array_key_exists('message', $returnArr)) {
            $ret['message'] = $returnArr['message'];
        }

        if(array_key_exists('data', $returnArr)) {
            $ret['data'] = $returnArr['data'];
        }

        return $ret;
    }


    /**
     * @param string $dsn
     * @param string $user
     * @param string $pass
     */
    public function connect($dsn, $user, $pass): void
    {

        $connArr['action'] = 'connect';
        $connArr['data'] = [
            'user' => $user,
            'pass' => $pass,
            'dsn' => $dsn
        ];
        fwrite($this->sock, json_encode($connArr)."\n");
        $this->parse_reply();
    }


    /**
     * @param string $query
     * @param array<string,string> $params
     * @return mixed
     * @throws Exception
     */
    public function exec(string $query, array $params = []): mixed
    {

        $sendArr['action'] = 'execute';
        $sendArr['data']['query'] = $query;

        if(!empty($params)) {

            foreach($params as $i=>$p) {
                $sendArr['data']['params'][$i] = $p;
            }
        }

        fwrite($this->sock, json_encode($sendArr)."\n");

        $return = $this->parse_reply();

        if(empty($return['message'])) {
            throw new Exception('ResultId is empty');
        }

        //this will return the result_id
        return $return['message'];
    }


    /**
     * @param string $resultId
     * @return mixed
     * @throws Exception
     */
    public function fetch_array(string $resultId): mixed
    {

        $sendArr['action'] = 'fetch_array';
        $sendArr['data']['result_id'] = $resultId;

        fwrite($this->sock, json_encode($sendArr)."\n");

        $return = $this->parse_reply();

        if(!array_key_exists('data', $return)) {
            throw new Exception('data is empty');
        }

        //this will return the result set
        return $return['data'];
    }

    /**
     * @param string $resultId
     * @return mixed
     * @throws Exception
     */
    public function free_result(string $resultId): mixed
    {

        $sendArr['action'] = 'free_result';
        $sendArr['data']['result_id'] = $resultId;

        fwrite($this->sock, json_encode($sendArr)."\n");
        return $this->parse_reply();
    }
}
