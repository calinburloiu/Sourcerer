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
package edu.uci.ics.sourcerer.eval.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface EvalServiceAsync {
  void getIntroductionText(AsyncCallback<String> callback);
  void getVoteOptions(AsyncCallback<Vote[]> callback);
  void getEvaluationProgress(String email, AsyncCallback<EvaluationProgress> callback);
  void getNextQuery(String email, String query, AsyncCallback<Query> callback);
  void getNextResult(String email, AsyncCallback<Result> callback);
  void reportVote(String email, String id, Vote vote, AsyncCallback<Void> callback);
}
