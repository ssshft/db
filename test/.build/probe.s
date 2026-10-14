	.section	__TEXT,__text,regular,pure_instructions
	.build_version macos, 15, 0	sdk_version 15, 5
	.globl	_probe_write                    ## -- Begin function probe_write
	.p2align	4, 0x90
_probe_write:                           ## @probe_write
Lfunc_begin0:
	.cfi_startproc
	.cfi_personality 155, ___gxx_personality_v0
	.cfi_lsda 16, Lexception0
## %bb.0:
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset %rbp, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register %rbp
	pushq	%r15
	pushq	%r14
	pushq	%r12
	pushq	%rbx
	.cfi_offset %rbx, -48
	.cfi_offset %r12, -40
	.cfi_offset %r14, -32
	.cfi_offset %r15, -24
	cmpq	%rdx, 200(%rdi)
	jae	LBB0_2
## %bb.1:
	lock		incq	192(%rdi)
	movl	$2, %eax
	jmp	LBB0_6
LBB0_2:
	movq	(%rdi), %r14
	movq	64(%rdi), %rax
	movq	%r14, %r8
	subq	%rax, %r8
	cmpq	$1024, %r8                      ## imm = 0x400
	jb	LBB0_4
## %bb.3:
	lock		incq	128(%rdi)
	movl	$1, %eax
	jmp	LBB0_6
LBB0_4:
	movq	%rcx, %r15
	movl	%r14d, %ebx
	andl	$1023, %ebx                     ## imm = 0x3FF
	shll	$6, %ebx
	movq	%rdi, %r12
	addq	208(%rdi), %rbx
Ltmp0:
	movq	%rbx, %rdi
	callq	__ZNSt3__112basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6assignEPKcm
Ltmp1:
## %bb.5:
	movq	%r15, 24(%rbx)
	incq	%r14
	movq	%r14, (%r12)
	xorl	%eax, %eax
LBB0_6:
	popq	%rbx
	popq	%r12
	popq	%r14
	popq	%r15
	popq	%rbp
	retq
LBB0_7:
Ltmp2:
	movq	%rax, %rdi
	callq	___clang_call_terminate
Lfunc_end0:
	.cfi_endproc
	.section	__TEXT,__gcc_except_tab
	.p2align	2, 0x0
GCC_except_table0:
Lexception0:
	.byte	255                             ## @LPStart Encoding = omit
	.byte	155                             ## @TType Encoding = indirect pcrel sdata4
	.uleb128 Lttbase0-Lttbaseref0
Lttbaseref0:
	.byte	1                               ## Call site Encoding = uleb128
	.uleb128 Lcst_end0-Lcst_begin0
Lcst_begin0:
	.uleb128 Ltmp0-Lfunc_begin0             ## >> Call Site 1 <<
	.uleb128 Ltmp1-Ltmp0                    ##   Call between Ltmp0 and Ltmp1
	.uleb128 Ltmp2-Lfunc_begin0             ##     jumps to Ltmp2
	.byte	1                               ##   On action: 1
Lcst_end0:
	.byte	1                               ## >> Action Record 1 <<
                                        ##   Catch TypeInfo 1
	.byte	0                               ##   No further actions
	.p2align	2, 0x0
                                        ## >> Catch TypeInfos <<
	.long	0                               ## TypeInfo 1
Lttbase0:
	.p2align	2, 0x0
                                        ## -- End function
	.section	__TEXT,__text,regular,pure_instructions
	.globl	_probe_peek                     ## -- Begin function probe_peek
	.p2align	4, 0x90
_probe_peek:                            ## @probe_peek
	.cfi_startproc
## %bb.0:
	movq	64(%rdi), %rax
	movq	(%rdi), %rcx
	cmpq	%rcx, %rax
	je	LBB1_2
## %bb.1:
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset %rbp, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register %rbp
	movl	%eax, %r8d
	andl	$1023, %r8d                     ## imm = 0x3FF
	movq	208(%rdi), %rdi
	shll	$6, %r8d
	leaq	(%rdi,%r8), %r9
	movq	%r9, (%rsi)
	movq	24(%rdi,%r8), %rsi
	movq	%rsi, (%rdx)
	popq	%rbp
LBB1_2:
	cmpq	%rcx, %rax
	setne	%al
	retq
	.cfi_endproc
                                        ## -- End function
	.globl	_probe_release                  ## -- Begin function probe_release
	.p2align	4, 0x90
_probe_release:                         ## @probe_release
	.cfi_startproc
## %bb.0:
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset %rbp, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register %rbp
	incq	64(%rdi)
	popq	%rbp
	retq
	.cfi_endproc
                                        ## -- End function
	.globl	_probe_readable                 ## -- Begin function probe_readable
	.p2align	4, 0x90
_probe_readable:                        ## @probe_readable
	.cfi_startproc
## %bb.0:
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset %rbp, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register %rbp
	movq	(%rdi), %rcx
	movq	64(%rdi), %rdx
	xorl	%eax, %eax
	subq	%rdx, %rcx
	cmovaeq	%rcx, %rax
	popq	%rbp
	retq
	.cfi_endproc
                                        ## -- End function
	.private_extern	___clang_call_terminate ## -- Begin function __clang_call_terminate
	.globl	___clang_call_terminate
	.weak_def_can_be_hidden	___clang_call_terminate
	.p2align	4, 0x90
___clang_call_terminate:                ## @__clang_call_terminate
	.cfi_startproc
## %bb.0:
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset %rbp, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register %rbp
	callq	___cxa_begin_catch
	callq	__ZSt9terminatev
	.cfi_endproc
                                        ## -- End function
.subsections_via_symbols
