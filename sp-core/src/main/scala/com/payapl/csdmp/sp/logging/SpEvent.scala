package com.payapl.csdmp.sp.logging

import com.fasterxml.jackson.annotation.JsonProperty


case class SpEvent(@JsonProperty("tenant_id") tenant_id: String,
                   channel: String,
                   date: Map[String, Any])
