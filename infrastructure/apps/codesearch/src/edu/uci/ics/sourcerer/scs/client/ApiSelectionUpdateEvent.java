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

package edu.uci.ics.sourcerer.scs.client;

import com.google.gwt.event.shared.GwtEvent;

import edu.uci.ics.sourcerer.scs.client.event.ApiSelectedEvent.Operation;
import edu.uci.ics.sourcerer.scs.common.client.EntityCategory;

/**
 * @author <a href="bajracharya@gmail.com">Sushil Bajracharya</a>
 * @created Aug 15, 2009
 */
public class ApiSelectionUpdateEvent extends
		GwtEvent<ApiSelectionUpdateEventHandler> {
	
	@Override
	protected void dispatch(ApiSelectionUpdateEventHandler handler) {
		handler.onApiSelectionUpdate(this);
	}

	@Override
	public GwtEvent.Type<ApiSelectionUpdateEventHandler> getAssociatedType() {
		return getType();
	}
	

	public static GwtEvent.Type<ApiSelectionUpdateEventHandler> getType() {
		return TYPE;
	}

	private static final GwtEvent.Type<ApiSelectionUpdateEventHandler> TYPE = 
		new GwtEvent.Type<ApiSelectionUpdateEventHandler>();
	
	public ApiSelectionUpdateEvent(String fqn, EntityCategory cat, Operation op){
		this.fqn = fqn;
		this.cat = cat;
		this.op = op;
	}
	
	public String fqn;
	public EntityCategory cat;
	public Operation op;
	
}
