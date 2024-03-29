// @flow
import React, { Component } from 'react'
import { graphql, compose } from 'react-apollo'
import { splitAt } from 'ramda'
import NoteItem from './item'
import NoteForm from './form'
import GetNotes from '../../queries/note_collections'

type Props = {
  data: Object,
  modelId: number,
  model: string,
  title: string,
  placeholderText?: string
}

type State = {
  prevNotesOpen: boolean
}

class NoteList extends Component<Props, State> {

  toggleNoteVisibilty: Function

  constructor(props: Object){
    super(props)

    this.state ={
      prevNotesOpen: false
    }

    this.toggleNoteVisibilty = this.toggleNoteVisibilty.bind(this)
  }

  toggleNoteVisibilty(){
    this.setState({ prevNotesOpen: !this.state.prevNotesOpen })
  }

  render(){
    const { loading, getNotes } = this.props.data
    if(loading){return <div></div>}
    const notes = getNotes.notes.length > 0 ? splitAt(getNotes.notes.length - 2, getNotes.notes) : null
    return(
      <div id={`${this.props.model}_${this.props.modelId}_notes`} className="notes">
        <div className="flex">
          <div className="mr-4 font-bold flex-1">{ this.props.title }</div>
          { getNotes.notes.length > 2 ?
            <div className="">
              <a onClick={ this.toggleNoteVisibilty }>View previous notes</a>
            </div>
          : null }
        </div>
        <div
          id={`${this.props.model}_${this.props.modelId}_prev_notes`}
          className={`prev-container ${ this.state.prevNotesOpen ? ' open' : '' }`}>
          { notes && notes[0].length > 0 ? notes[0].map((note, i) => (
            <div key={`${this.props.model}_${this.props.modelId}_note_${note.id}_prev_${i}`} className="mb-4">
              <NoteItem
                note={note}
                model={ this.props.model }
                modelId={ this.props.modelId } />
            </div>
          )) : null }
        </div>
        <div id={`${this.props.model}_${this.props.modelId}_new_notes`}>
          { notes && notes[1].length > 0 ? notes[1].map((note, i) => (
            <div key={`${this.props.model}_${this.props.modelId}_note_${note.id}_new_${i}`} >
              <NoteItem
                note={ note }
                model={ this.props.model }
                modelId={ this.props.modelId } />
            </div>
          )) : null }
        </div>
        <div id={`${this.props.model}_${this.props.modelId}_notes_form`}>
          <NoteForm
            model={ this.props.model }
            modelId={this.props.modelId }
            placeholderText={ this.props.placeholderText }
           />
        </div>
      </div>
    )
  }
}

export default graphql(GetNotes, {
  options: (props) => ({
    variables: {
      input: {
        model: props.model,
        modelId: props.modelId } } })
})(NoteList)
