package jbossews;
/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 
 * @author Adam Lev-Libfeld @ Tamar Tech
 * 
 */
@SuppressWarnings("serial")
@WebServlet("/AMPQ")
public class AMPQServlet extends HttpServlet {

    static String PAGE_HEADER = "<html><head><title>AMPQ</title></head><body>";

    static String PAGE_FOOTER = "</body></html>";


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        PrintWriter writer = resp.getWriter();
        writer.println(PAGE_HEADER);
        
        String retval;
        try {
        	retval = "<h1>" +  AMPQConnectorService.run() + "</h1>";
		} catch (Exception e) {
			// TODO Auto-generated catch block
			retval = e.toString();
		}
        writer.println(retval);
        writer.println(PAGE_FOOTER);
        writer.close();
    }

}