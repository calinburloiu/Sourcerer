/*
 * Sourcerer: An infrastructure for large-scale source code analysis.
 * Copyright (C) by contributors. See CONTRIBUTORS.txt for full list.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.uci.ics.sourcerer.scs.common.client;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="bajracharya@gmail.com">Sushil Bajracharya</a>
 * @created Aug 10, 2009 
 */
public class HitFqnEntityId implements Serializable {
	
	private static final long serialVersionUID = -6045501180020701258L;
	
	public String fqn;
	public String entityId;
	public String useCount;
	public int rank;
	public String score;
	public List<String> snippetParts;
	
	public HitFqnEntityId(){
		
	}
	
	public HitFqnEntityId(String fqn, String eid){
		this.fqn = fqn;
		this.entityId = eid;
	}
	
	public HitFqnEntityId(String fqn, String eid, String useCount){
		this.fqn = fqn;
		this.entityId = eid;
		this.useCount = useCount;
	}
	
	public int getUseCount(){
		return Integer.parseInt(useCount);
	}
}
