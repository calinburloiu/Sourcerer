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
package edu.uci.ics.sourcerer.db.adapter.client;

import java.io.Serializable;

import edu.uci.ics.sourcerer.scs.common.client.EntityCategory;
import edu.uci.ics.sourcerer.scs.common.client.EntityType;

/**
 * @author <a href="bajracharya@gmail.com">Sushil Bajracharya</a>
 * @created Jul 20, 2009
 */
public class Entity implements Serializable{
	
	private static final long serialVersionUID = -675123763872482506L;
	
	public String fqn;
	public EntityType type;
	public EntityCategory category;
	public long entityId;
	public long parentId;
	public int useCount;
	
	
}
