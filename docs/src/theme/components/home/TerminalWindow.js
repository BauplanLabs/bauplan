import { Terminal as BashTerminal, TerminalLine, BauplanAsciiArt, Description } from '@site/src/theme/components/home/TerminalComponents';

export const TerminalWindow = () => {
    return (
        <BashTerminal>
            <BauplanAsciiArt />

            <Description>
                Git-for-data branching and concurrent data operations at scale
            </Description>

            <TerminalLine command="pip install bauplan" />
            <TerminalLine command="bauplan checkout -b your_data_branch" />
            <TerminalLine command="bauplan run" showCursor={true} />

        </BashTerminal>
    )
}
